# SPDX-FileCopyrightText: 2026-present Adrian Rumpold <a.rumpold@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0
import math

import pytest
from haystack import Document
from haystack.document_stores.types import DocumentStore, DuplicatePolicy
from haystack.testing.document_store import DocumentStoreBaseTests
from typing_extensions import override

from haystack_integrations.document_stores.duckdb import DuckDBDocumentStore


def _documents_equal_with_tolerance(doc1: Document, doc2: Document, rtol: float = 1e-5) -> bool:
    """
    Compare two documents for equality, allowing floating-point tolerance for embeddings.

    DuckDB stores embeddings as FLOAT (32-bit) which loses precision compared to Python's
    64-bit floats. We compare embeddings with relative tolerance instead of exact equality.
    """
    # Compare all non-embedding fields exactly
    if doc1.id != doc2.id:
        return False
    if doc1.content != doc2.content:
        return False
    if doc1.meta != doc2.meta:
        return False
    if doc1.blob != doc2.blob:
        return False
    if doc1.score != doc2.score:
        return False

    # Compare embeddings with tolerance
    if doc1.embedding is None and doc2.embedding is None:
        return True
    if doc1.embedding is None or doc2.embedding is None:
        return False
    if len(doc1.embedding) != len(doc2.embedding):
        return False

    for v1, v2 in zip(doc1.embedding, doc2.embedding, strict=False):
        if not math.isclose(v1, v2, rel_tol=rtol):
            return False

    return True


class TestDocumentStore(DocumentStoreBaseTests):
    """
    Common test cases will be provided by `DocumentStoreBaseTests` but
    you can add more to this class.
    """

    @override
    @pytest.fixture
    def document_store(self) -> DuckDBDocumentStore:
        return DuckDBDocumentStore(table="documents", recreate_index=True, recreate_table=True)

    @override
    def assert_documents_are_equal(self, received: list[Document], expected: list[Document]):
        """
        Assert that two lists of Documents are equal (order-independent).

        DuckDB does not guarantee result ordering without explicit ORDER BY,
        so we compare sorted lists. This override is explicitly permitted by
        the base class docstring for stores with different ordering behavior.

        Additionally, DuckDB stores embeddings as FLOAT32 which loses precision
        compared to Python's 64-bit floats, so we use tolerance-based comparison.
        """
        sorted_received = sorted(received, key=lambda d: d.id)
        sorted_expected = sorted(expected, key=lambda d: d.id)

        assert len(sorted_received) == len(sorted_expected), (
            f"Different number of documents: received {len(sorted_received)}, expected {len(sorted_expected)}"
        )

        for i, (r, e) in enumerate(zip(sorted_received, sorted_expected, strict=False)):
            assert _documents_equal_with_tolerance(r, e), (
                f"Documents at index {i} are not equal:\n  received: {r}\n  expected: {e}"
            )

    @override
    def test_write_documents(self, document_store: DocumentStore):
        # TODO: Determine behavior without a policy set
        pass

    def test_write_documents_batch(self, document_store: DocumentStore):
        docs = [Document(id="1", content="test doc 1"), Document(id="2", content="test doc 2")]

        assert document_store.write_documents(docs, policy=DuplicatePolicy.FAIL) == len(docs)
