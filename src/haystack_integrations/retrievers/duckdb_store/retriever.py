# SPDX-FileCopyrightText: 2026-present Adrian Rumpold <a.rumpold@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0
from haystack import component
from typing_extensions import Any

from haystack_integrations.document_stores.duckdb import DuckDBDocumentStore


@component
class DuckDBRetriever:
    """
    A component for retrieving documents from a DuckDBDocumentStore.
    """

    def __init__(self, document_store: DuckDBDocumentStore, filters: dict[str, Any] | None = None, top_k: int = 10):
        """
        Create a DuckDBRetriever component. Usually you pass some basic configuration
        parameters to the constructor.

        :param document_store: A Document Store object used to retrieve documents
        :param filters: A dictionary with filters to narrow down the search space (default is None).
        :param top_k: The maximum number of documents to retrieve (default is 10).

        :raises ValueError: If the specified top_k is not > 0.
        """
        self.filters = filters
        self.top_k = top_k
        self.document_store = document_store

    def run(self, _):
        """
        Run the Retriever on the given input data.

        :param data: The input data for the retriever. In this case, a list of queries.
        :return: The retrieved documents.
        """
        return []  # FIXME
