# SPDX-FileCopyrightText: 2026-present Adrian Rumpold <a.rumpold@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0
import math

import pytest
from haystack import Document
from haystack.dataclasses import ByteStream
from haystack.document_stores.types import DuplicatePolicy

from haystack_integrations.document_stores.duckdb import DuckDBDocumentStore


@pytest.fixture
def embedding_store(request: pytest.FixtureRequest) -> DuckDBDocumentStore:
    """Provide a document store with a small embedding dimension for testing.

    Supports indirect parametrization via a dict with optional keys:
    - similarity_metric: SimilarityMetric (default: "cosine")
    - embedding_dim: int (default: 3)
    """
    params: dict = getattr(request, "param", {})
    return DuckDBDocumentStore(
        table="test_embedding",
        index="idx_test_embedding",
        embedding_dim=params.get("embedding_dim", 3),
        similarity_metric=params.get("similarity_metric", "cosine"),
        recreate_table=True,
        recreate_index=True,
    )


@pytest.mark.integration
class TestEmbeddingRetrieval:
    """Integration tests for embedding retrieval functionality."""

    def test_embedding_retrieval_basic(self, embedding_store: DuckDBDocumentStore):
        """Test basic embedding retrieval without filters."""
        docs = [
            Document(id="1", content="first document", embedding=[1.0, 0.0, 0.0]),
            Document(id="2", content="second document", embedding=[0.0, 1.0, 0.0]),
            Document(id="3", content="third document", embedding=[0.0, 0.0, 1.0]),
        ]
        embedding_store.write_documents(docs, policy=DuplicatePolicy.FAIL)

        # Query with an embedding close to the first document
        results = embedding_store.embedding_retrieval([0.9, 0.1, 0.0], top_k=2)

        assert len(results) == 2
        assert results[0].id == "1"
        assert results[0].content == "first document"

    @pytest.mark.parametrize(
        ("num_docs", "top_k", "expected"),
        [
            (5, 3, 3),
            (5, 10, 5),  # top_k > num_docs returns all docs
            (5, 1, 1),
        ],
    )
    def test_embedding_retrieval_top_k(
        self, embedding_store: DuckDBDocumentStore, num_docs: int, top_k: int, expected: int
    ):
        """Test that top_k parameter limits results correctly."""
        docs = [Document(id=f"{i}", content=f"document {i}", embedding=[float(i), 0.0, 0.0]) for i in range(num_docs)]
        embedding_store.write_documents(docs, policy=DuplicatePolicy.FAIL)

        results = embedding_store.embedding_retrieval([2.5, 0.0, 0.0], top_k=top_k)

        assert len(results) == expected

    @pytest.mark.parametrize(
        "embedding_store",
        [
            {"similarity_metric": "cosine"},
            {"similarity_metric": "l2sq"},
            {"similarity_metric": "ip"},
        ],
        indirect=True,
        ids=["cosine", "l2sq", "ip"],
    )
    def test_embedding_retrieval_with_different_metrics(self, embedding_store: DuckDBDocumentStore):
        """Test embedding retrieval with different similarity metrics."""
        docs = [
            Document(id="1", content="doc1", embedding=[1.0, 0.0, 0.0]),
            Document(id="2", content="doc2", embedding=[0.0, 1.0, 0.0]),
        ]
        embedding_store.write_documents(docs, policy=DuplicatePolicy.FAIL)

        results = embedding_store.embedding_retrieval([1.0, 0.0, 0.0], top_k=1)

        assert len(results) == 1
        assert results[0].id == "1"

    def test_embedding_retrieval_empty_store(self, embedding_store: DuckDBDocumentStore):
        """Test embedding retrieval on an empty document store."""
        results = embedding_store.embedding_retrieval([1.0, 0.0, 0.0], top_k=10)
        assert len(results) == 0

    def test_embedding_retrieval_preserves_metadata(self, embedding_store: DuckDBDocumentStore):
        """Test that embedding retrieval preserves document metadata."""
        docs = [
            Document(
                id="1",
                content="test document",
                embedding=[1.0, 0.0, 0.0],
                meta={"author": "Alice", "date": "2026-01-01"},
            ),
        ]
        embedding_store.write_documents(docs, policy=DuplicatePolicy.FAIL)

        results = embedding_store.embedding_retrieval([1.0, 0.0, 0.0], top_k=1)

        assert len(results) == 1
        assert results[0].meta == {"author": "Alice", "date": "2026-01-01"}

    def test_embedding_retrieval_with_blob_data(self, embedding_store: DuckDBDocumentStore):
        """Test embedding retrieval with documents containing blob data."""
        blob_data = b"binary content"
        blob = ByteStream(data=blob_data, mime_type="application/octet-stream")
        docs = [
            Document(
                id="1",
                content="document with blob",
                embedding=[1.0, 0.0, 0.0],
                blob=blob,
            ),
        ]
        embedding_store.write_documents(docs, policy=DuplicatePolicy.FAIL)

        results = embedding_store.embedding_retrieval([1.0, 0.0, 0.0], top_k=1)

        assert len(results) == 1
        assert results[0].blob is not None
        assert results[0].blob.data == blob_data
        assert results[0].blob.mime_type == blob.mime_type

    def test_embedding_retrieval_returns_embeddings(self, embedding_store: DuckDBDocumentStore):
        """Test that retrieved documents contain their embeddings."""
        original_embedding = [0.5, 0.5, 0.0]
        docs = [
            Document(id="1", content="test", embedding=original_embedding),
        ]
        embedding_store.write_documents(docs, policy=DuplicatePolicy.FAIL)

        results = embedding_store.embedding_retrieval([0.5, 0.5, 0.0], top_k=1)

        assert len(results) == 1
        assert results[0].embedding is not None
        # Check with tolerance due to float32 precision
        for v1, v2 in zip(results[0].embedding, original_embedding, strict=False):
            assert math.isclose(v1, v2, rel_tol=1e-5)

    def test_embedding_retrieval_with_filters(self, embedding_store: DuckDBDocumentStore):
        """Test embedding retrieval with filters."""
        docs = [Document(id=f"doc_{i}", content=f"Document {i}", embedding=[float(i), 0.0, 0.0]) for i in range(10)]

        for i in range(10):
            docs[i].meta["meta_field"] = "custom_value" if i % 2 == 0 else "other_value"

        embedding_store.write_documents(docs, policy=DuplicatePolicy.FAIL)

        query_embedding = [2.5, 0.0, 0.0]
        filters = {"field": "meta.meta_field", "operator": "==", "value": "custom_value"}

        results = embedding_store.embedding_retrieval(query_embedding, filters=filters, top_k=3)

        assert len(results) == 3
        for result in results:
            assert result.meta["meta_field"] == "custom_value"

    def test_embedding_retrieval_similarity_score_order(self, embedding_store: DuckDBDocumentStore):
        """Test that similarity scores are ordered correctly (cosine: higher is better)."""
        query_embedding = [0.1, 0.0, 0.0]
        most_similar_embedding = [0.8, 0.0, 0.0]
        second_best_embedding = [0.5, 0.0, 0.0]
        least_similar_embedding = [0.0, 0.8, 0.0]

        docs = [
            Document(id="1", content="Most similar", embedding=most_similar_embedding),
            Document(id="2", content="Second best", embedding=second_best_embedding),
            Document(id="3", content="Not similar", embedding=least_similar_embedding),
        ]
        embedding_store.write_documents(docs, policy=DuplicatePolicy.FAIL)

        results = embedding_store.embedding_retrieval(query_embedding, top_k=2)

        assert len(results) == 2
        assert results[0].content == "Most similar"
        assert results[1].content == "Second best"
        assert results[0].score is not None
        assert results[1].score is not None
        assert results[0].score >= results[1].score

    def test_embedding_retrieval_l2_distance_score_order(self):
        """Test that L2 distance scores are ordered correctly (lower is better)."""
        store = DuckDBDocumentStore(
            table="test_l2",
            index="idx_test_l2",
            embedding_dim=3,
            similarity_metric="l2sq",
            recreate_table=True,
            recreate_index=True,
        )

        query_embedding = [0.1, 0.0, 0.0]
        most_similar_embedding = [0.15, 0.0, 0.0]  # Distance ~0.0025
        second_best_embedding = [0.3, 0.0, 0.0]  # Distance ~0.04
        least_similar_embedding = [1.0, 0.0, 0.0]  # Distance ~0.81

        docs = [
            Document(id="1", content="Most similar", embedding=most_similar_embedding),
            Document(id="2", content="Second best", embedding=second_best_embedding),
            Document(id="3", content="Not similar", embedding=least_similar_embedding),
        ]
        store.write_documents(docs, policy=DuplicatePolicy.FAIL)

        results = store.embedding_retrieval(query_embedding, top_k=2)

        assert len(results) == 2
        assert results[0].content == "Most similar"
        assert results[1].content == "Second best"

    def test_embedding_retrieval_with_complex_filters(self, embedding_store: DuckDBDocumentStore):
        """Test embedding retrieval with nested AND/OR filters."""
        docs = []
        for i in range(10):
            doc = Document(
                id=f"doc_{i}",
                content=f"Document {i}",
                embedding=[float(i), 0.0, 0.0],
                meta={
                    "category": "A" if i < 5 else "B",
                    "score": i,
                },
            )
            docs.append(doc)

        embedding_store.write_documents(docs, policy=DuplicatePolicy.FAIL)

        query_embedding = [5.0, 0.0, 0.0]
        filters = {
            "operator": "AND",
            "conditions": [
                {"field": "meta.category", "operator": "==", "value": "B"},
                {"field": "meta.score", "operator": ">=", "value": 7},
            ],
        }

        results = embedding_store.embedding_retrieval(query_embedding, filters=filters, top_k=10)

        # Should only return documents with category B and score >= 7 (docs 7, 8, 9)
        assert len(results) == 3
        for result in results:
            assert result.meta["category"] == "B"
            assert result.meta["score"] >= 7
