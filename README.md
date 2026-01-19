[![PyPI - Version](https://img.shields.io/pypi/v/duckdb-haystack)](https://pypi.org/project/duckdb-haystack/)
[![GitHub License](https://img.shields.io/github/license/AdrianoKF/duckdb-haystack)](https://github.com/AdrianoKF/duckdb-haystack/blob/main/LICENSE)
[![CI Test Status](https://github.com/AdrianoKF/duckdb-haystack/actions/workflows/test.yml/badge.svg)](https://github.com/AdrianoKF/duckdb-haystack/actions/workflows/test.yml)

# DuckDB Document Store for Haystack

> [!NOTE]
> This project is a proof of concept - use at your own risk.
> The code may be susceptible to bugs and security issues (such as SQL injection), proceed with caution.

A DuckDB-backed document store for [Haystack](https://github.com/deepset-ai/haystack/) with
HNSW vector search via DuckDB's [VSS](https://duckdb.org/docs/stable/core_extensions/vss) extension. It supports:

- Dense embedding storage with HNSW indexing (cosine similarity, Euclidean distance, or inner product distance)
- Filtering with Haystack-style filter dictionaries
- In-memory operation or persistence via a DuckDB database file on disk

## Installation (GitHub)

Use `uv` to install directly from the repository:

```console
uv pip install "duckdb-haystack @ git+https://github.com/AdrianoKF/duckdb-haystack.git"
```

## Usage

### 1) DocumentStore CRUD example

```python
from haystack import Document

from haystack_integrations.document_stores.duckdb import DuckDBDocumentStore, document_store

store = DuckDBDocumentStore(
    database=":memory:",
    embedding_dim=3,
    similarity_metric="cosine",
)

store.write_documents(
    [
        Document(id="doc-1", content="DuckDB is fast.", embedding=[0.1, 0.0, 0.9], meta={"source": "notes"}),
        Document(id="doc-2", content="Haystack pipelines are modular.", embedding=[0.2, 0.1, 0.8]),
    ]
)

print("Total document count:", store.count_documents())

filters = {"field": "meta.source", "operator": "==", "value": "notes"}
filtered = store.filter_documents(filters=filters)
print("Filtered documents:", [doc.id for doc in filtered])

store.delete_documents(document_ids=["doc-2"])
print("After deletion:", store.filter_documents())
```

### 2) Retrieval with DuckDBRetriever in a pipeline

```python
from haystack import Document, Pipeline
from haystack.components.embedders import SentenceTransformersDocumentEmbedder, SentenceTransformersTextEmbedder

from haystack_integrations.document_stores.duckdb import DuckDBDocumentStore
from haystack_integrations.retrievers.duckdb import DuckDBRetriever

store = DuckDBDocumentStore(database=":memory:", embedding_dim=384)

doc_embedder = SentenceTransformersDocumentEmbedder(model="sentence-transformers/all-MiniLM-L6-v2")
doc_embedder.warm_up()
documents = [
    Document(content="DuckDB stores vectors in Float arrays backed by an HNSW index."),
    Document(content="DuckDB is an analytical in-process SQL database management system."),
    Document(content="Haystack offers composable pipelines."),
]
documents = doc_embedder.run(documents=documents)["documents"]
store.write_documents(documents)

query_embedder = SentenceTransformersTextEmbedder(model="sentence-transformers/all-MiniLM-L6-v2")
retriever = DuckDBRetriever(document_store=store)

pipeline = Pipeline()
pipeline.add_component("query_embedder", query_embedder)
pipeline.add_component("retriever", retriever)
pipeline.connect("query_embedder.embedding", "retriever.query_embedding")

result = pipeline.run(data={"query_embedder": {"text": "How does DuckDB store vectors?"}})

print(result["retriever"]["documents"][0].content)
```

## License

`duckdb-haystack` is distributed under the terms of the [Apache-2.0](https://spdx.org/licenses/Apache-2.0.html)
license.
