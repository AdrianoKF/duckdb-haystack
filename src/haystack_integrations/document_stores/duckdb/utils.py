import json
import logging

from haystack.dataclasses import ByteStream, Document
from typing_extensions import Any

logger = logging.getLogger(__name__)


def validate_table_name(name: str) -> bool:
    # FIXME
    return True


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
        breakpoint()
        haystack_dict = dict(document)
        blob_data = haystack_dict.pop("blob_data", None)
        blob_meta = haystack_dict.pop("blob_meta", None)
        blob_mime_type = haystack_dict.pop("blob_mime_type", None)

        haystack_dict["embedding"] = document["embedding"]

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
