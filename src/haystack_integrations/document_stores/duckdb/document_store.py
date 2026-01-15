# SPDX-FileCopyrightText: 2026-present Adrian Rumpold <a.rumpold@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0
import logging
from pathlib import Path
from typing import Any, Literal

from haystack import Document, default_from_dict, default_to_dict
from haystack.document_stores.errors import DuplicateDocumentError, MissingDocumentError
from haystack.document_stores.types import DuplicatePolicy

import duckdb
from haystack_integrations.document_stores.duckdb.utils import (
    to_duckdb_documents,
    to_haystack_documents,
    validate_table_name,
)

logger = logging.getLogger(__name__)


_CREATE_TABLE_QUERY = """
CREATE TABLE {table} (
id VARCHAR(128) PRIMARY KEY,
embedding FLOAT[{embedding_dim}],
content TEXT,
blob_data BYTEA,
blob_meta JSON,
blob_mime_type VARCHAR(255),
meta JSON)
"""

_CREATE_INDEX_QUERY = """
CREATE INDEX {index}
ON {table}
USING HNSW(embedding)
"""

_UPDATE_QUERY = """
ON CONFLICT (id) DO UPDATE SET
embedding = EXCLUDED.embedding,
content = EXCLUDED.content,
blob_data = EXCLUDED.blob_data,
blob_meta = EXCLUDED.blob_meta,
blob_mime_type = EXCLUDED.blob_mime_type,
meta = EXCLUDED.meta
"""


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
        similarity_metric: Literal["l2sq", "cosine", "ip"] = "cosine",
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

        if not validate_table_name(table):
            msg = f"invalid table nam: {table!r}"
            raise ValueError(msg)

        if not validate_table_name(index):
            msg = f"invalid index name: {index!r}"
            raise ValueError(msg)

        self.table = table
        self.index = index

        self.recreate_table = recreate_table
        self.recreate_index = recreate_index
        self.create_index_if_missing = create_index_if_missing

        self.embedding_dim = embedding_dim

        # NOTE: Need to explicitly enable persistent HNSW indices, still an experimental feature
        self._db = duckdb.connect(
            database,
            config={
                "hnsw_enable_experimental_persistence": True,
            },
        )
        self._table_initialized = False

        self._db.install_extension("vss")
        self._db.load_extension("vss")

        self._ensure_db_setup()

    def _delete_table(self):
        logger.debug(f"Dropping table {self.table!r}")
        self._db.execute(f"DROP TABLE IF EXISTS {self.table}")

    def _delete_index(self):
        logger.debug(f"Dropping index {self.index!r}")
        self._db.execute(f"DROP INDEX IF EXISTS {self.index}")

    def _create_index(self):
        if self.recreate_index:
            self._delete_index()
        logger.debug(f"Creating index {self.index!r}")
        self._db.execute(_CREATE_INDEX_QUERY.format(index=self.index, table=self.table))

    def _create_table(self):
        try:
            if self.recreate_table:
                self._delete_table()

            logger.debug(f"Creating table {self.table!r}")
            self._db.execute(
                _CREATE_TABLE_QUERY.format(
                    table=self.table,
                    embedding_dim=self.embedding_dim,
                ),
            )
            self._table_initialized = True
        except duckdb.DatabaseError:
            logger.error(f"could not create database table {self.table!r}", exc_info=True)

    def _ensure_db_setup(self):
        if not self._table_initialized:
            self._create_table()

        index_exists = False
        if not index_exists:
            if not self.create_index_if_missing:
                msg = f"index is missing, but create_index_if_missing=False: {self.index!r}"
                raise RuntimeError(msg)
            self._create_index()

    def count_documents(self) -> int:
        """
        Returns how many documents are present in the document store.
        """
        self._ensure_db_setup()

        return int(self._db.table(self.table).count("*").fetchone()[0])

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
        _ = filters

        # TODO: Apply filters
        columns = ["id", "embedding", "content", "blob_data", "blob_meta", "blob_mime_type", "meta"]
        records = self._db.execute(f"SELECT {", ".join(columns)} FROM {self.table}").fetchmany()
        docs = [{col: val for rec in records for col, val in zip(columns, rec, strict=True)}]
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
        num_written = 0

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
            policy_action = _UPDATE_QUERY

        self._db.begin()
        try:
            for doc in to_duckdb_documents(documents):
                num_rows = self._db.execute(
                    f"INSERT INTO {self.table} (id, embedding, content, blob_data, blob_meta, blob_mime_type, meta) VALUES ($id, $embedding, $content, $blob_data, $blob_meta, $blob_mime_type, $meta) {policy_action}",
                    parameters=doc,
                ).fetchone()[0]
                num_written += num_rows
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

        self._db.execute(f"DELETE FROM {self.table} WHERE id in ?", [document_ids])

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
