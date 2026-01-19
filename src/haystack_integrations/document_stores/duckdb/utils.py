import json
import logging
import re
from functools import reduce

from haystack.dataclasses import ByteStream, Document
from haystack.errors import FilterError
from typing_extensions import Any

from duckdb import ColumnExpression, ConstantExpression, Expression, FunctionExpression
from duckdb.sqltypes import DOUBLE

logger = logging.getLogger(__name__)


def is_valid_identifier(name: str) -> bool:
    """Validate that a table/index name is safe to use as an identifier."""
    if not name:
        return False
    # Allow alphanumeric characters and underscores, must start with letter or underscore
    if not name[0].isalpha() and name[0] != "_":
        return False
    return all(c.isalnum() or c == "_" for c in name)


def quote_identifier(name: str) -> str:
    """
    Quote an identifier for safe use in SQL statements.

    This function escapes double quotes within the identifier and wraps it
    in double quotes, following SQL standard identifier quoting rules.

    :param name: The identifier to quote (table name, column name, index name, etc.)
    :return: The quoted identifier safe for use in SQL
    :raises ValueError: If the identifier is empty or fails validation
    """
    if not is_valid_identifier(name):
        msg = f"Invalid identifier: {name!r}"
        raise ValueError(msg)
    # Escape any double quotes by doubling them, then wrap in double quotes
    return '"' + name.replace('"', '""') + '"'


def _is_iso8601_datetime(s: str) -> bool:
    """Check if a string looks like an ISO 8601 datetime."""

    # Basic ISO 8601 datetime pattern: YYYY-MM-DDTHH:MM:SS with optional timezone
    iso_pattern = r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$"
    return bool(re.match(iso_pattern, s))


def _build_comparison_expression(filter_condition: dict[str, Any]) -> Expression:
    """
    Build a DuckDB Expression from a comparison filter condition.

    :param filter_condition: A dict with 'field', 'operator', and 'value' keys.
    :return: A DuckDB Expression representing the comparison.
    :raises FilterError: If the filter condition is invalid.
    """
    try:
        field = filter_condition["field"]
        operator = filter_condition["operator"]
        value = filter_condition["value"]
    except KeyError as e:
        msg = f"Missing required key in filter condition: {e}"
        raise FilterError(msg) from e

    # Handle meta.* fields via JSON extraction (returns text, not JSON)
    is_meta_field = field.startswith("meta.")
    if is_meta_field:
        json_path = "$." + field[5:]  # Remove 'meta.' prefix, add '$.' for JSONPath
        # Use json_extract_string (->> operator) to get text values for comparison
        col_expr = FunctionExpression(
            "json_extract_string",
            ColumnExpression("meta"),
            ConstantExpression(json_path),
        )
    else:
        col_expr = ColumnExpression(field)

    # Validate types for comparison operators
    # Numeric comparison operators should not accept list values or arbitrary string values
    # Exception: ISO 8601 datetime strings are allowed (lexicographic comparison works correctly)
    numeric_operators = (">", ">=", "<", "<=")
    if operator in numeric_operators:
        if isinstance(value, list):
            msg = f"Operator '{operator}' does not support list values"
            raise FilterError(msg)
        if isinstance(value, str) and not _is_iso8601_datetime(value):
            msg = f"Operator '{operator}' does not support string values (except ISO 8601 datetime)"
            raise FilterError(msg)

    # Helper to get a properly typed column expression for numeric comparisons
    def get_numeric_col_expr(v: Any) -> Expression:
        """Return a column expression cast to DOUBLE if the value is numeric and field is from meta."""
        if is_meta_field and isinstance(v, int | float) and not isinstance(v, bool):
            return col_expr.cast(DOUBLE)
        return col_expr

    # Map Haystack operators to DuckDB expressions
    if operator == "==":
        if value is None:
            return col_expr.isnull()
        value_expr = ConstantExpression(value)
        return get_numeric_col_expr(value) == value_expr
    elif operator == "!=":
        if value is None:
            return col_expr.isnotnull()
        value_expr = ConstantExpression(value)
        # In Python, None != value is True, but in SQL, NULL != value is NULL (unknown)
        # To match Python semantics: (field != value) OR (field IS NULL)
        return (get_numeric_col_expr(value) != value_expr) | col_expr.isnull()
    elif operator == ">":
        value_expr = ConstantExpression(value)
        return get_numeric_col_expr(value) > value_expr
    elif operator == ">=":
        value_expr = ConstantExpression(value)
        return get_numeric_col_expr(value) >= value_expr
    elif operator == "<":
        value_expr = ConstantExpression(value)
        return get_numeric_col_expr(value) < value_expr
    elif operator == "<=":
        value_expr = ConstantExpression(value)
        return get_numeric_col_expr(value) <= value_expr
    elif operator == "in":
        if not isinstance(value, list):
            msg = f"'in' operator requires a list value, got {type(value).__name__}"
            raise FilterError(msg)
        # Check if any value in the list is numeric (excluding bools)
        has_numeric = any(isinstance(v, int | float) and not isinstance(v, bool) for v in value)
        target_col = col_expr.cast(DOUBLE) if is_meta_field and has_numeric else col_expr
        return target_col.isin(*[ConstantExpression(v) for v in value])
    elif operator == "not in":
        if not isinstance(value, list):
            msg = f"'not in' operator requires a list value, got {type(value).__name__}"
            raise FilterError(msg)
        # Check if any value in the list is numeric (excluding bools)
        has_numeric = any(isinstance(v, int | float) and not isinstance(v, bool) for v in value)
        target_col = col_expr.cast(DOUBLE) if is_meta_field and has_numeric else col_expr
        # To match Python semantics: (field NOT IN list) OR (field IS NULL)
        return target_col.isnotin(*[ConstantExpression(v) for v in value]) | col_expr.isnull()
    else:
        msg = f"Unsupported filter operator: {operator!r}"
        raise FilterError(msg)


def build_filter_expression(filters: dict[str, Any]) -> Expression | None:
    """
    Convert a Haystack filter dictionary to a DuckDB Expression.

    Supports both comparison filters (with 'field', 'operator', 'value') and
    logic filters (with 'operator', 'conditions').

    :param filters: A Haystack filter dictionary. Empty dict returns None.
    :return: A DuckDB Expression representing the filter, or None if filters is empty.
    :raises FilterError: If the filter structure is invalid.
    """
    # Empty filter dict means no filtering
    if not filters:
        return None

    if "conditions" in filters:
        # Logic filter (AND/OR/NOT)
        if "operator" not in filters:
            msg = "Logic filter must have an 'operator' key"
            raise FilterError(msg)

        operator = filters["operator"]
        conditions = filters["conditions"]

        if not conditions:
            msg = "Logic filter must have at least one condition"
            raise FilterError(msg)

        # Recursively build expressions for all conditions
        sub_expressions = [build_filter_expression(c) for c in conditions]
        # Filter out None values (from empty nested filters)
        sub_expressions = [e for e in sub_expressions if e is not None]

        if not sub_expressions:
            return None

        if operator == "AND":
            return reduce(lambda a, b: a & b, sub_expressions)
        elif operator == "OR":
            return reduce(lambda a, b: a | b, sub_expressions)
        elif operator == "NOT":
            # NOT with multiple conditions means NOT (cond1 AND cond2 AND ...)
            combined = reduce(lambda a, b: a & b, sub_expressions)
            # DuckDB uses three-valued logic; treat NULL as False before negating.
            return ~combined | combined.isnull()
        else:
            msg = f"Unsupported logic operator: {operator!r}"
            raise FilterError(msg)
    elif "field" in filters:
        # Comparison filter
        return _build_comparison_expression(filters)
    else:
        msg = f"Invalid filter structure: {filters!r}"
        raise FilterError(msg)


def to_duckdb_documents(documents: list[Document]) -> list[dict[str, Any]]:
    """
    Internal method to convert a list of Haystack Documents to a list of dictionaries that can be used to insert
    documents into the PgvectorDocumentStore.
    """

    db_documents = []
    for document in documents:
        db_document = {k: v for k, v in document.to_dict(flatten=False).items() if k not in ["score", "blob"]}

        blob = document.blob
        db_document["blob_data"] = blob.data if blob else None
        db_document["blob_meta"] = blob.meta if blob and blob.meta else None
        db_document["blob_mime_type"] = blob.mime_type if blob and blob.mime_type else None

        if "sparse_embedding" in db_document:
            sparse_embedding = db_document.pop("sparse_embedding", None)
            if sparse_embedding:
                logger.warning(
                    f"Document {db_document['id']} has the `sparse_embedding` field set,"
                    "but storing sparse embeddings in DuckDB is not currently supported."
                    "The `sparse_embedding` field will be ignored.",
                )

        db_documents.append(db_document)

    return db_documents


def to_haystack_documents(documents: list[dict[str, Any]]) -> list[Document]:
    """
    Internal method to convert a list of dictionaries from DuckDB to a list of Haystack Documents.
    """

    haystack_documents = []
    if documents == [{}]:
        return haystack_documents
    for document in documents:
        haystack_dict = dict(document)
        blob_data = haystack_dict.pop("blob_data", None)
        blob_meta = haystack_dict.pop("blob_meta", None)
        blob_mime_type = haystack_dict.pop("blob_mime_type", None)

        # DuckDB returns FLOAT[] arrays as tuples, but Haystack expects lists
        embedding = document["embedding"]
        haystack_dict["embedding"] = list(embedding) if embedding is not None else None

        # Document.from_dict expects the meta field to be a a dict or not be present (not None)
        if "meta" in haystack_dict and haystack_dict["meta"] is None:
            haystack_dict.pop("meta")
        else:
            haystack_dict["meta"] = json.loads(haystack_dict["meta"])

        haystack_document = Document.from_dict(haystack_dict)

        if blob_data:
            blob = ByteStream(data=blob_data, meta=blob_meta, mime_type=blob_mime_type)
            haystack_document.blob = blob

        haystack_documents.append(haystack_document)

    return haystack_documents
