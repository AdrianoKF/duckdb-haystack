# SPDX-FileCopyrightText: 2026-present Adrian Rumpold <a.rumpold@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

from haystack import Document, component, default_from_dict, default_to_dict
from haystack.document_stores.types import FilterPolicy
from haystack.document_stores.types.filter_policy import apply_filter_policy
from typing_extensions import Any

from haystack_integrations.document_stores.duckdb import DuckDBDocumentStore
from haystack_integrations.document_stores.duckdb.document_store import SimilarityMetric


@component
class DuckDBRetriever:
    """
    A component for retrieving documents from a DuckDBDocumentStore.
    """

    def __init__(
        self,
        document_store: DuckDBDocumentStore,
        filters: dict[str, Any] | None = None,
        filter_policy: FilterPolicy = FilterPolicy.REPLACE,
        similarity_metric: SimilarityMetric = "cosine",
        top_k: int = 10,
    ):
        """
        Create a DuckDBRetriever component. Usually you pass some basic configuration
        parameters to the constructor.

        :param document_store: A Document Store object used to retrieve documents
        :param filters: A dictionary with filters to narrow down the search space (default is None).
        :param top_k: The maximum number of documents to retrieve (default is 10).

        :raises ValueError: If the specified top_k is not > 0.
        """
        self.filters = filters
        self.filter_policy = filter_policy
        self.similarity_metric: SimilarityMetric = similarity_metric
        self.top_k = top_k
        self.document_store = document_store

    @component.output_types(documents=list[Document])
    def run(
        self,
        query_embedding: list[float],
        filters: dict[str, Any] | None = None,
        top_k: int | None = None,
        similarity_metric: SimilarityMetric | None = None,
    ) -> dict[str, list[Document]]:
        """
        Retrieve documents from the `DuckDBDocumentStore`, based on their embeddings.

        :param data: The input data for the retriever. In this case, a list of queries.
        :return: The retrieved documents.
        """
        top_k = top_k or self.top_k
        similarity_metric = similarity_metric or self.similarity_metric
        filters = apply_filter_policy(self.filter_policy, self.filters, filters)

        documents = self.document_store.embedding_retrieval(
            query_embedding,
            filters=filters,
            top_k=top_k,
            similarity_metric=similarity_metric,
        )

        return {"documents": documents}

    def to_dict(self) -> dict[str, Any]:
        """
        Serializes the component to a dictionary.

        :returns:
            Dictionary with serialized data.
        """
        return default_to_dict(
            self,
            document_store=self.document_store.to_dict(),
            filters=self.filters,
            filter_policy=self.filter_policy.value,
            similarity_metric=self.similarity_metric,
            top_k=self.top_k,
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DuckDBRetriever":
        """
        Deserializes the component from a dictionary.

        :param data:
            Dictionary to deserialize from.
        :returns:
            Deserialized component.
        """
        init_params = data.get("init_parameters", {})
        if "document_store" in init_params:
            init_params["document_store"] = DuckDBDocumentStore.from_dict(init_params["document_store"])
        if "filter_policy" in init_params:
            init_params["filter_policy"] = FilterPolicy.from_str(init_params["filter_policy"])
        return default_from_dict(cls, data)
