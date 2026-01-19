# SPDX-FileCopyrightText: 2026-present Adrian Rumpold <a.rumpold@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0
import json
import logging
from collections.abc import Sequence
from pathlib import Path
from typing import Literal, Protocol, TypeAlias

import pandas as pd
from haystack import Document, default_from_dict, default_to_dict
from haystack.document_stores.errors import DuplicateDocumentError
from haystack.document_stores.types import DuplicatePolicy
from typing_extensions import Any

import duckdb
from haystack_integrations.document_stores.duckdb.utils import (
    build_filter_expression,
    is_valid_identifier,
    quote_identifier,
    to_duckdb_documents,
    to_haystack_documents,
)

logger = logging.getLogger(__name__)


_CREATE_TABLE_QUERY = """\
CREATE TABLE {table} (
"id" VARCHAR(128) PRIMARY KEY,
{embedding_field} FLOAT[{embedding_dim}],
"content" TEXT,
"blob_data" BYTEA,
"blob_meta" JSON,
"blob_mime_type" VARCHAR(255),
"meta" JSON)
"""

_CREATE_INDEX_QUERY = """\
CREATE INDEX {index}
ON {table}
USING HNSW({embedding_field})
WITH (metric = '{metric}')
"""

_INSERT_QUERY = """\
INSERT INTO {table}
("id", {embedding_field}, "content", "blob_data", "blob_meta", "blob_mime_type", "meta")
VALUES ($id, $embedding, $content, $blob_data, $blob_meta, $blob_mime_type, $meta)
{policy_action}
"""

_UPDATE_QUERY_FRAGMENT = """\
ON CONFLICT ("id") DO UPDATE SET
{embedding_field} = EXCLUDED.{embedding_field},
"content" = EXCLUDED."content",
"blob_data" = EXCLUDED."blob_data",
"blob_meta" = EXCLUDED."blob_meta",
"blob_mime_type" = EXCLUDED."blob_mime_type",
"meta" = EXCLUDED."meta"
"""


SimilarityMetric: TypeAlias = Literal["l2sq", "cosine", "ip"]


def _metric_to_distance_sql_function(metric: SimilarityMetric) -> str:
    match metric:
        case "l2sq":
            return "array_distance"
        case "cosine":
            return "array_cosine_distance"
        case "ip":
            return "array_negative_inner_product"
        case _:
            msg = f"invalid similarity metric: {metric!r}"
            raise ValueError(msg)


def _metric_to_score_sql_expression(metric: SimilarityMetric, distance_expression: str) -> str:
    match metric:
        case "cosine":
            return f"(1 - {distance_expression})"
        case "l2sq" | "ip":
            return f"(-{distance_expression})"
        case _:
            msg = f"invalid similarity metric: {metric!r}"
            raise ValueError(msg)


class FilterCondition(Protocol):
    field: str
    operator: Literal["==", "!=", "<", "<=", ">", ">=", "in", "not in"]
    value: Any


class LogicExpression(Protocol):
    operator: Literal["AND", "OR", "NOT"]
    conditions: Sequence[FilterCondition]


class DuckDBDocumentStore:
    """
    Except for the __init__(), signatures of any other method in this class must not change.
    """

    def __init__(
        self,
        *,
        database: str | Path = ":memory:",
        table: str = "haystack_documents",
        index: str = "hnsw_idx_haystack_documents",
        embedding_dim: int = 768,
        embedding_field: str = "embedding",
        similarity_metric: SimilarityMetric = "cosine",
        # progress_bar: bool = False,
        write_batch_size: int = 100,
        create_index_if_missing: bool = True,
        recreate_table: bool = False,
        recreate_index: bool = False,
    ):
        """
        Initializes the store. The __init__ constructor is not part of the Store Protocol
        and the signature can be customized to your needs. For example, parameters needed
        to set up a database client would be passed to this method.
        """
        super().__init__()

        if not is_valid_identifier(table):
            msg = f"invalid table name: {table!r}"
            raise ValueError(msg)

        if not is_valid_identifier(index):
            msg = f"invalid index name: {index!r}"
            raise ValueError(msg)

        self.table = table
        self.index = index

        self.recreate_table = recreate_table
        self.recreate_index = recreate_index
        self.create_index_if_missing = create_index_if_missing

        self.embedding_field = embedding_field
        self.embedding_dim = embedding_dim
        self.similarity_metric: SimilarityMetric = similarity_metric  # NOTE: why does this need an explicit type hint?

        self.write_batch_size = write_batch_size

        # NOTE: Need to explicitly enable persistent HNSW indices, still an experimental feature
        self._db = duckdb.connect(
            database,
            config={
                "hnsw_enable_experimental_persistence": True,
            },
        )
        self._table_initialized = False
        self._index_initialized = False

        self._db.install_extension("vss")
        self._db.load_extension("vss")

        self._ensure_db_setup()

    def _execute_query(
        self,
        query: str,
        params: dict | list | None = None,
        *,
        operation: str = "query",
        explain: bool = False,
    ) -> duckdb.DuckDBPyConnection:
        """
        Execute a SQL query with logging and error handling.

        :param query: The SQL query to execute.
        :param params: Query parameters (dict for named parameters, list for positional).
        :param operation: Description of the operation for logging purposes.
        :param explain: If True, run EXPLAIN on the query first and log the result.
        :return: DuckDB result object.
        """
        try:
            # Log query (truncate if too long)
            max_preview_length = 200
            query_preview = (
                query[:max_preview_length].replace("\n", " ")
                if len(query) > max_preview_length
                else query.replace("\n", " ")
            )
            logger.debug(f"Executing {operation}: {query_preview}{'...' if len(query) > max_preview_length else ''}")

            # Run EXPLAIN if requested
            if explain:
                # Use EXPLAIN ANALYZE for detailed execution plan with actual timings
                explain_query = f"EXPLAIN ANALYZE {query}"
                explain_result = self._db.execute(explain_query, params)
                # Get the plan as a DataFrame and extract the explain_value column
                explain_df = explain_result.df()
                if not explain_df.empty and "explain_value" in explain_df.columns:
                    plan_text = explain_df["explain_value"].iloc[0]
                    logger.debug(f"Query plan for {operation}:")
                    for line in str(plan_text).split("\n"):
                        logger.debug(f"  {line}")

            return self._db.execute(query, params)
        except duckdb.Error as e:
            logger.error(f"Database error during {operation}: {e}", exc_info=True)
            raise

    def _delete_table(self):
        logger.debug(f"Dropping table {self.table!r}")
        self._execute_query(
            f"DROP TABLE IF EXISTS {quote_identifier(self.table)}",
            operation="drop table",
        )

    def _delete_index(self):
        logger.debug(f"Dropping index {self.index!r}")
        self._execute_query(
            f"DROP INDEX IF EXISTS {quote_identifier(self.index)}",
            operation="drop index",
        )

    def _create_index(self):
        logger.debug(f"Creating index {self.index!r}")
        self._execute_query(
            _CREATE_INDEX_QUERY.format(
                index=quote_identifier(self.index),
                table=quote_identifier(self.table),
                embedding_field=quote_identifier(self.embedding_field),
                metric=self.similarity_metric,
            ),
            operation="create index",
        )

    def _index_exists(self) -> bool:
        result = self._execute_query(
            "SELECT COUNT(*) FROM duckdb_indexes() WHERE index_name = ?",
            [self.index],
            operation="check index exists",
        ).fetchone()
        return bool(result and result[0])

    def _table_exists(self) -> bool:
        result = self._execute_query(
            "SELECT COUNT(*) FROM duckdb_tables() WHERE table_name = ?",
            [self.table],
            operation="check table exists",
        ).fetchone()
        return bool(result and result[0])

    def _create_table(self):
        try:
            if self.recreate_table:
                self._delete_table()

            if self._table_exists():
                logger.debug(f"Table {self.table!r} already exists")
                self._table_initialized = True
                return

            logger.debug(f"Creating table {self.table!r}")
            self._execute_query(
                _CREATE_TABLE_QUERY.format(
                    table=quote_identifier(self.table),
                    embedding_field=quote_identifier(self.embedding_field),
                    embedding_dim=self.embedding_dim,
                ),
                operation="create table",
            )
            self._table_initialized = True
        except duckdb.DatabaseError:
            logger.error(f"could not create database table {self.table!r}", exc_info=True)

    def _ensure_db_setup(self):
        if not self._table_initialized:
            self._create_table()

        if not self._index_initialized:
            if self.recreate_index:
                self._delete_index()
                self._create_index()
                self._index_initialized = True
                return

            if self._index_exists():
                self._index_initialized = True
                return

            if not self.create_index_if_missing:
                msg = f"index is missing, but create_index_if_missing=False: {self.index!r}"
                raise RuntimeError(msg)

            self._create_index()
            self._index_initialized = True

    def count_documents(self) -> int:
        """
        Returns how many documents are present in the document store.
        """
        self._ensure_db_setup()

        result = self._db.table(self.table).count("*").fetchone()
        if not result:
            raise RuntimeError
        return int(result[0])

    def filter_documents(self, filters: dict[str, Any] | None = None) -> list[Document]:
        """
        Returns the documents that match the filters provided.

        Filters are defined as nested dictionaries that can be of two types:
        - Comparison
        - Logic

        Comparison dictionaries must contain the keys:

        - `field`
        - `operator`
        - `value`

        Logic dictionaries must contain the keys:

        - `operator`
        - `conditions`

        The `conditions` key must be a list of dictionaries, either of type Comparison or Logic.

        The `operator` value in Comparison dictionaries must be one of:

        - `==`
        - `!=`
        - `>`
        - `>=`
        - `<`
        - `<=`
        - `in`
        - `not in`

        The `operator` values in Logic dictionaries must be one of:

        - `NOT`
        - `OR`
        - `AND`


        A simple filter:
        ```python
        filters = {"field": "meta.type", "operator": "==", "value": "article"}
        ```

        A more complex filter:
        ```python
        filters = {
            "operator": "AND",
            "conditions": [
                {"field": "meta.type", "operator": "==", "value": "article"},
                {"field": "meta.date", "operator": ">=", "value": 1420066800},
                {"field": "meta.date", "operator": "<", "value": 1609455600},
                {"field": "meta.rating", "operator": ">=", "value": 3},
                {
                    "operator": "OR",
                    "conditions": [
                        {"field": "meta.genre", "operator": "in", "value": ["economy", "politics"]},
                        {"field": "meta.publisher", "operator": "==", "value": "nytimes"},
                    ],
                },
            ],
        }

        :param filters: the filters to apply to the document list.
        :return: a list of Documents that match the given filters.
        """
        self._ensure_db_setup()

        columns = ["id", "embedding", "content", "blob_data", "blob_meta", "blob_mime_type", "meta"]
        select_columns = [
            "id",
            self.embedding_field,
            "content",
            "blob_data",
            "blob_meta",
            "blob_mime_type",
            "meta",
        ]

        base_relation = self._db.table(self.table).set_alias("docs")
        relation = base_relation

        # Apply filters if provided. Use a filtered-id join to avoid DuckDB dropping array columns.
        # See issue: https://github.com/duckdb/duckdb/issues/20579
        if filters is not None:
            filter_expr = build_filter_expression(filters)
            if filter_expr is not None:
                filtered_ids = base_relation.filter(filter_expr).project("id").set_alias("filtered_ids")
                relation = base_relation.join(filtered_ids, "id")

        relation = relation.select(*[f"docs.{col}" for col in select_columns])

        # Convert to SQL and execute
        query = relation.sql_query()
        records = self._execute_query(query, operation="filter documents").fetchall()
        docs = [dict(zip(columns, rec, strict=True)) for rec in records]
        return to_haystack_documents(docs)

    def write_documents(self, documents: list[Document], policy: DuplicatePolicy = DuplicatePolicy.NONE) -> int:
        """
        Writes (or overwrites) documents into the store.

        :param documents: a list of documents.
        :param policy: documents with the same ID count as duplicates. When duplicates are met,
            the store can:
             - skip: keep the existing document and ignore the new one.
             - overwrite: remove the old document and write the new one.
             - fail: an error is raised
        :raises DuplicateDocumentError: Exception trigger on duplicate document if `policy=DuplicatePolicy.FAIL`
        :return: number of documents written to the store
        """

        # Defensive type checks for input arguments
        if not isinstance(documents, list):
            msg = f"documents is not a list: {documents!r}"
            raise ValueError(msg)

        for doc in documents:
            if not isinstance(doc, Document):
                msg = f"document is not an instance of Document: {doc!r}"
                raise ValueError(msg)

        policy_action = ""
        if policy in (DuplicatePolicy.SKIP, DuplicatePolicy.NONE):
            policy_action = "ON CONFLICT DO NOTHING"
        elif policy == DuplicatePolicy.OVERWRITE:
            policy_action = _UPDATE_QUERY_FRAGMENT.format(embedding_field=quote_identifier(self.embedding_field))

        insert_columns = [
            "id",
            self.embedding_field,
            "content",
            "blob_data",
            "blob_meta",
            "blob_mime_type",
            "meta",
        ]
        source_columns = [
            "id",
            "embedding",
            "content",
            "blob_data",
            "blob_meta",
            "blob_mime_type",
            "meta",
        ]
        insert_cols_sql = ", ".join(quote_identifier(col) for col in insert_columns)
        source_cols_sql = ", ".join(quote_identifier(col) for col in source_columns)
        policy_clause = f" {policy_action}" if policy_action else ""
        batch_size = max(1, self.write_batch_size)
        view_name = "_haystack_docs_batch"
        view_name_sql = quote_identifier(view_name)

        num_written = 0
        self._db.begin()
        try:
            db_documents = to_duckdb_documents(documents)
            for doc in db_documents:
                if doc.get("meta") is not None and not isinstance(doc["meta"], str):
                    doc["meta"] = json.dumps(doc["meta"])
                if doc.get("blob_meta") is not None and not isinstance(doc["blob_meta"], str):
                    doc["blob_meta"] = json.dumps(doc["blob_meta"])

            for start in range(0, len(db_documents), batch_size):
                batch = db_documents[start : start + batch_size]
                df = pd.DataFrame.from_records(batch, columns=source_columns)
                self._db.register(view_name, df)
                try:
                    result = self._execute_query(
                        f"INSERT INTO {quote_identifier(self.table)} ({insert_cols_sql}) "
                        f"SELECT {source_cols_sql} FROM {view_name_sql}{policy_clause} RETURNING 1",
                        operation="insert documents batch",
                    ).fetchall()
                finally:
                    self._db.unregister(view_name)
                num_written += len(result)
            self._db.commit()
        except duckdb.ConstraintException as ce:
            self._db.rollback()
            raise DuplicateDocumentError from ce

        return num_written

    def delete_documents(self, document_ids: list[str]) -> None:
        """
        Deletes all documents with a matching document_ids from the document store.
        Fails with `MissingDocumentError` if no document with this id is present in the store.

        :param object_ids: the object_ids to delete
        """
        self._ensure_db_setup()

        # FIXME: check for existence
        self._db.fetchone()

        query = f'DELETE FROM {quote_identifier(self.table)} WHERE "id" IN ?'
        self._execute_query(query, [document_ids], operation="delete documents")

    def to_dict(self) -> dict[str, Any]:
        """
        Serializes this store to a dictionary. You can customize here what goes into the
        final serialized format.
        """
        # FIXME: Add remaining instance attributes
        data = default_to_dict(
            self,
            table=self.table,
            index=self.index,
        )
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DuckDBDocumentStore":
        """
        Deserializes the store from a dictionary, if you customized anything in `to_dict`,
        you can changed it back here.
        """
        return default_from_dict(cls, data)

    def embedding_retrieval(
        self,
        query_embedding: Sequence[float],
        *,
        filters: dict[str, Any] | None = None,
        similarity_metric: SimilarityMetric | None = None,
        top_k: int = 10,
    ) -> list[Document]:
        self._ensure_db_setup()

        # Column names for result dict (standardized to "embedding")
        columns = ["id", "embedding", "content", "blob_data", "blob_meta", "blob_mime_type", "meta", "score"]
        # DB columns to select (may use custom embedding_field)
        select_columns = [
            "id",
            self.embedding_field,
            "content",
            "blob_data",
            "blob_meta",
            "blob_mime_type",
            "meta",
        ]

        similarity_metric = similarity_metric or self.similarity_metric
        distance_fn = _metric_to_distance_sql_function(similarity_metric)
        embedding_field_quoted = quote_identifier(self.embedding_field)
        table_quoted = quote_identifier(self.table)
        distance_expr = f"{distance_fn}({embedding_field_quoted}, $query_embedding::FLOAT[{self.embedding_dim}])"
        score_expr = _metric_to_score_sql_expression(similarity_metric, distance_expr)
        select_sql_parts = [quote_identifier(col) for col in select_columns]
        select_sql_parts.append(f"{score_expr} AS score")
        select_sql = ", ".join(select_sql_parts)

        # Apply filters if provided using filtered-id subquery
        where_clause = ""
        params = {"query_embedding": query_embedding}

        if filters is not None:
            filter_expr = build_filter_expression(filters)
            if filter_expr is not None:
                # Use DuckDB relational API to build filtered IDs subquery
                base_relation = self._db.table(self.table)
                filtered_ids_relation = base_relation.filter(filter_expr).project("id")
                filtered_ids_sql = filtered_ids_relation.sql_query()

                # Add WHERE clause to restrict to filtered IDs
                where_clause = f' WHERE "id" IN ({filtered_ids_sql})'

        # Build and execute the final query with distance ordering
        query = f"""
SELECT {select_sql}
FROM {table_quoted}
{where_clause}
ORDER BY {distance_expr} ASC
LIMIT {top_k}
"""
        result = self._execute_query(query, params, operation="embedding retrieval")
        records = result.fetchall()
        docs = [dict(zip(columns, rec, strict=True)) for rec in records]
        return to_haystack_documents(docs)
