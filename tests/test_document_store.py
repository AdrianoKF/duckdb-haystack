# SPDX-FileCopyrightText: 2026-present Adrian Rumpold <a.rumpold@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0
import pytest
from haystack import Document
from haystack.document_stores.types import DocumentStore, DuplicatePolicy
from haystack.testing.document_store import DocumentStoreBaseTests
from typing_extensions import override

from haystack_integrations.document_stores.duckdb import DuckDBDocumentStore


class TestDocumentStore(DocumentStoreBaseTests):
    """
    Common test cases will be provided by `DocumentStoreBaseTests` but
    you can add more to this class.
    """

    @override
    @pytest.fixture
    def document_store(self) -> DuckDBDocumentStore:
        """
        This is the most basic requirement for the child class: provide
        an instance of this document store so the base class can use it.
        """
        return DuckDBDocumentStore(table="documents", recreate_index=True, recreate_table=True)

    @override
    def test_write_documents(self, document_store: DocumentStore):
        # TODO: Determine behavior without a policy set
        pass

    def test_write_documents_batch(self, document_store: DocumentStore):
        docs = [Document(id="1", content="test doc 1"), Document(id="2", content="test doc 2")]

        assert document_store.write_documents(docs, policy=DuplicatePolicy.FAIL) == len(docs)
