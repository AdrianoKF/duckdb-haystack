#!/usr/bin/env python3
import argparse
import time

from haystack.testing.document_store import create_filterable_docs

from haystack_integrations.document_stores.duckdb import DuckDBDocumentStore


def _now() -> float:
    return time.perf_counter()


def _fmt(seconds: float) -> str:
    return f"{seconds * 1000:.1f} ms"


def run_once(
    *,
    reuse_store: DuckDBDocumentStore | None,
    database: str,
    recreate_table: bool,
    recreate_index: bool,
) -> dict[str, float]:
    timings: dict[str, float] = {}

    start = _now()
    docs = create_filterable_docs()
    timings["create_docs"] = _now() - start

    if reuse_store is None:
        start = _now()
        store = DuckDBDocumentStore(
            database=database,
            table="documents",
            recreate_table=recreate_table,
            recreate_index=recreate_index,
        )
        timings["init_store"] = _now() - start
    else:
        store = reuse_store

    start = _now()
    store.write_documents(docs)
    timings["write_documents"] = _now() - start

    start = _now()
    store.filter_documents(
        filters={
            "operator": "NOT",
            "conditions": [
                {"field": "meta.number", "operator": "==", "value": 100},
                {"field": "meta.name", "operator": "==", "value": "name_0"},
            ],
        }
    )
    timings["filter_documents"] = _now() - start

    return timings


def main() -> None:
    parser = argparse.ArgumentParser(description="Profile DuckDBDocumentStore insert/filter timings.")
    parser.add_argument("--iterations", type=int, default=3, help="Number of iterations to run.")
    parser.add_argument("--database", default=":memory:", help="DuckDB database path.")
    parser.add_argument("--reuse-store", action="store_true", help="Reuse a single store across iterations.")
    parser.add_argument("--no-recreate", action="store_true", help="Disable table/index recreation on init.")
    args = parser.parse_args()

    recreate_table = not args.no_recreate
    recreate_index = not args.no_recreate

    store: DuckDBDocumentStore | None = None
    if args.reuse_store:
        store = DuckDBDocumentStore(
            database=args.database,
            table="documents",
            recreate_table=recreate_table,
            recreate_index=recreate_index,
        )

    totals: dict[str, float] = {}
    for i in range(args.iterations):
        timings = run_once(
            reuse_store=store,
            database=args.database,
            recreate_table=recreate_table,
            recreate_index=recreate_index,
        )
        for key, value in timings.items():
            totals[key] = totals.get(key, 0.0) + value

        formatted = ", ".join(f"{k}={_fmt(v)}" for k, v in timings.items())
        print(f"iter {i + 1}: {formatted}")

    avg = ", ".join(f"{k}={_fmt(v / args.iterations)}" for k, v in totals.items())
    print(f"avg ({args.iterations}): {avg}")


if __name__ == "__main__":
    main()
