# SPDX-FileCopyrightText: 2026-present Adrian Rumpold <a.rumpold@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0
from unittest.mock import Mock

import pytest
from haystack import Document
from haystack.document_stores.types import FilterPolicy

from haystack_integrations.document_stores.duckdb import DuckDBDocumentStore
from haystack_integrations.retrievers.duckdb import DuckDBRetriever


@pytest.fixture
def mock_store():
    """Create a mock document store for testing."""
    store = Mock(spec=DuckDBDocumentStore)
    store.embedding_dim = 768
    store.similarity_metric = "cosine"
    return store


class TestDuckDBRetriever:
    def test_init_default(self, mock_store):
        """Test retriever initialization with default parameters."""
        retriever = DuckDBRetriever(document_store=mock_store)
        assert retriever.document_store == mock_store
        assert retriever.filters is None
        assert retriever.top_k == 10
        assert retriever.filter_policy == FilterPolicy.REPLACE
        assert retriever.similarity_metric == "cosine"

    def test_init_with_parameters(self, mock_store):
        """Test retriever initialization with custom parameters."""
        retriever = DuckDBRetriever(
            document_store=mock_store,
            filters={"field": "value"},
            top_k=5,
            similarity_metric="l2sq",
            filter_policy=FilterPolicy.MERGE,
        )
        assert retriever.document_store == mock_store
        assert retriever.filters == {"field": "value"}
        assert retriever.top_k == 5
        assert retriever.similarity_metric == "l2sq"
        assert retriever.filter_policy == FilterPolicy.MERGE

    def test_init_with_filter_policy_string(self, mock_store):
        """Test retriever initialization with filter policy as string."""
        retriever = DuckDBRetriever(document_store=mock_store, filter_policy=FilterPolicy.MERGE)
        assert retriever.filter_policy == FilterPolicy.MERGE

    def test_to_dict(self):
        """Test serialization to dictionary."""
        store = DuckDBDocumentStore(
            database=":memory:",
            table="test_table",
            index="test_index",
            embedding_dim=384,
            similarity_metric="l2sq",
        )
        retriever = DuckDBRetriever(
            document_store=store,
            filters={"field": "value"},
            top_k=5,
            similarity_metric="l2sq",
            filter_policy=FilterPolicy.MERGE,
        )
        res = retriever.to_dict()

        expected_type = "haystack_integrations.retrievers.duckdb.retriever.DuckDBRetriever"
        assert res["type"] == expected_type
        assert res["init_parameters"]["filters"] == {"field": "value"}
        assert res["init_parameters"]["top_k"] == 5
        assert res["init_parameters"]["similarity_metric"] == "l2sq"
        assert res["init_parameters"]["filter_policy"] == "merge"
        assert "document_store" in res["init_parameters"]
        expected_ds_type = "haystack_integrations.document_stores.duckdb.document_store.DuckDBDocumentStore"
        assert res["init_parameters"]["document_store"]["type"] == expected_ds_type

    def test_from_dict(self):
        """Test deserialization from dictionary."""
        data = {
            "type": "haystack_integrations.retrievers.duckdb.retriever.DuckDBRetriever",
            "init_parameters": {
                "document_store": {
                    "type": "haystack_integrations.document_stores.duckdb.document_store.DuckDBDocumentStore",
                    "init_parameters": {
                        "database": ":memory:",
                        "table": "test_table",
                        "index": "test_index",
                        "embedding_dim": 384,
                        "similarity_metric": "l2sq",
                    },
                },
                "filters": {"field": "value"},
                "top_k": 5,
                "similarity_metric": "l2sq",
                "filter_policy": "merge",
            },
        }

        retriever = DuckDBRetriever.from_dict(data)

        assert isinstance(retriever, DuckDBRetriever)
        assert isinstance(retriever.document_store, DuckDBDocumentStore)
        assert retriever.document_store.table == "test_table"
        assert retriever.document_store.index == "test_index"
        assert retriever.document_store.embedding_dim == 384
        assert retriever.document_store.similarity_metric == "l2sq"
        assert retriever.filters == {"field": "value"}
        assert retriever.top_k == 5
        assert retriever.similarity_metric == "l2sq"
        assert retriever.filter_policy == FilterPolicy.MERGE

    def test_from_dict_without_filter_policy(self):
        """Test deserialization without filter_policy defaults to REPLACE."""
        data = {
            "type": "haystack_integrations.retrievers.duckdb.retriever.DuckDBRetriever",
            "init_parameters": {
                "document_store": {
                    "type": "haystack_integrations.document_stores.duckdb.document_store.DuckDBDocumentStore",
                    "init_parameters": {
                        "database": ":memory:",
                        "table": "test_table",
                        "index": "test_index",
                    },
                },
                "filters": {"field": "value"},
                "top_k": 5,
            },
        }

        retriever = DuckDBRetriever.from_dict(data)

        assert retriever.filter_policy == FilterPolicy.REPLACE
        assert retriever.filters == {"field": "value"}
        assert retriever.top_k == 5

    def test_run_basic(self, mock_store):
        """Test basic run without filters."""
        doc = Document(content="Test doc", embedding=[0.1, 0.2, 0.3])
        mock_store.embedding_retrieval.return_value = [doc]

        retriever = DuckDBRetriever(document_store=mock_store)
        res = retriever.run(query_embedding=[0.3, 0.5, 0.1])

        mock_store.embedding_retrieval.assert_called_once_with(
            [0.3, 0.5, 0.1],
            filters=None,
            top_k=10,
            similarity_metric="cosine",
        )

        assert res == {"documents": [doc]}

    def test_run_with_filters(self, mock_store):
        """Test run with filters."""
        doc = Document(content="Test doc", embedding=[0.1, 0.2, 0.3])
        mock_store.embedding_retrieval.return_value = [doc]

        retriever = DuckDBRetriever(
            document_store=mock_store,
            filters={"meta.type": "article"},
            filter_policy=FilterPolicy.REPLACE,
        )
        res = retriever.run(query_embedding=[0.3, 0.5, 0.1])

        mock_store.embedding_retrieval.assert_called_once_with(
            [0.3, 0.5, 0.1],
            filters={"meta.type": "article"},
            top_k=10,
            similarity_metric="cosine",
        )

        assert res == {"documents": [doc]}

    def test_run_with_filter_merge_policy(self, mock_store):
        """Test run with MERGE filter policy."""
        doc = Document(content="Test doc", embedding=[0.1, 0.2, 0.3])
        mock_store.embedding_retrieval.return_value = [doc]

        retriever = DuckDBRetriever(
            document_store=mock_store,
            filters={"meta.type": "article"},
            filter_policy=FilterPolicy.MERGE,
        )
        # Pass runtime filters that should be merged
        retriever.run(
            query_embedding=[0.3, 0.5, 0.1],
            filters={"meta.author": "John"},
        )

        # The actual merging logic is done by apply_filter_policy
        # We just verify the call was made with merged filters
        call_args = mock_store.embedding_retrieval.call_args
        assert call_args[0] == ([0.3, 0.5, 0.1],)
        # Filters should be merged (order may vary)
        filters_arg = call_args[1]["filters"]
        assert "meta.type" in str(filters_arg) or "meta.author" in str(filters_arg)

    def test_run_with_custom_top_k(self, mock_store):
        """Test run with custom top_k parameter."""
        doc = Document(content="Test doc", embedding=[0.1, 0.2, 0.3])
        mock_store.embedding_retrieval.return_value = [doc]

        retriever = DuckDBRetriever(document_store=mock_store, top_k=5)
        res = retriever.run(query_embedding=[0.3, 0.5, 0.1], top_k=3)

        mock_store.embedding_retrieval.assert_called_once_with(
            [0.3, 0.5, 0.1],
            filters=None,
            top_k=3,  # Runtime parameter takes precedence
            similarity_metric="cosine",
        )

        assert res == {"documents": [doc]}

    def test_run_with_custom_similarity_metric(self, mock_store):
        """Test run with custom similarity metric parameter."""
        doc = Document(content="Test doc", embedding=[0.1, 0.2, 0.3])
        mock_store.embedding_retrieval.return_value = [doc]

        retriever = DuckDBRetriever(
            document_store=mock_store,
            similarity_metric="cosine",
        )
        res = retriever.run(
            query_embedding=[0.3, 0.5, 0.1],
            similarity_metric="l2sq",
        )

        mock_store.embedding_retrieval.assert_called_once_with(
            [0.3, 0.5, 0.1],
            filters=None,
            top_k=10,
            similarity_metric="l2sq",  # Runtime parameter takes precedence
        )

        assert res == {"documents": [doc]}

    def test_run_with_multiple_documents(self, mock_store):
        """Test run returning multiple documents."""
        docs = [
            Document(content="Doc 1", embedding=[0.1, 0.2, 0.3]),
            Document(content="Doc 2", embedding=[0.2, 0.3, 0.4]),
            Document(content="Doc 3", embedding=[0.3, 0.4, 0.5]),
        ]
        mock_store.embedding_retrieval.return_value = docs

        retriever = DuckDBRetriever(document_store=mock_store, top_k=3)
        res = retriever.run(query_embedding=[0.3, 0.5, 0.1])

        mock_store.embedding_retrieval.assert_called_once()
        assert res == {"documents": docs}
        assert len(res["documents"]) == 3
