# Retrieval brain — design note (epic is-565)

Status: design / pre-build. is-565.1 (`get_session`) is DONE (branch
`is-565.1-get-session`). This note covers the two remaining children:
is-565.2 (retrieve+rerank pipeline) and is-565.3 (supersession/trust ranking).

## The one architectural decision the pebbles get wrong: storage engine

The pebbles say "sqlite-vec + all-MiniLM-L6-v2". **This repo's store is
DuckDB, not SQLite.** Bolting on sqlite-vec means running two database
engines side by side and keeping them in sync — a dual-store with a
sync burden, against the "the thing you test is the thing you ship"
principle.

**Decision: use DuckDB-native vector search, not sqlite-vec.**

- Stage-1 vector: DuckDB `vss` extension (HNSW index) + `array_cosine_distance`
  over a `FLOAT[384]` embedding column on (or beside) `records`.
- Stage-1 lexical: DuckDB `fts` extension (BM25) over `records.raw` / message text.
- Fusion: Reciprocal Rank Fusion (RRF) of the two candidate lists — pure code.
- Stage-2 rerank: cross-encoder `ms-marco-MiniLM-L-6-v2` jointly scoring
  (query, candidate). Pretrained, zero project labels at cold start.

"sqlite-vec" in the pebble was prior-art shorthand (ClawMem is SQLite-based);
the embedding model and two-stage shape carry over, the engine does not.

## Scale reality (measured)

Runtime DB today: **857,771 records across 7,649 sessions**. This is why
two-stage is mandatory (don't rerank-all) — and why the build is non-trivial:

- Embedding 857k records is the heavy one-time cost. Needs a batched,
  resumable backfill (embed only new/changed records on subsequent runs,
  same incremental discipline as `file_mtimes`).
- Model deps: `all-MiniLM-L6-v2` (~90MB) + cross-encoder (~90MB). Prefer an
  ONNX runtime (`fastembed`) over torch to keep the on-device install light
  and the per-prompt hook sub-second. This is a real new dependency surface.

## Sub-second budget

The UserPromptSubmit/SessionStart hook is synchronous. Query-time path:
embed the query (1 short text), ANN search (HNSW), BM25, RRF, rerank top-N
(N≈50). Query-embedding + rerank of ~50 candidates is the cost; backfill is
offline. Needs a latency check against the real 857k-row index, not a fixture.

## is-565.3 ranking (the differentiator)

Compose over the is-565.2 candidate scores:
`final = relevance × recency_decay × confidence × not_superseded`, with a
**trust-tier** floor so an LCM auto-summary (sprig/bindle) never outranks a
contradicting primary-source record.

- SUPERSESSION: ingest `A supersedes B` links in a GENERAL form (a new table,
  e.g. `supersessions(superseded_uuid, superseding_uuid, source, created_at)`),
  fed by per-consumer adapters OUTSIDE this repo (git reverts / tracker
  closes / ADR amendments). Derive trivial cases internally where observable.
  Down-weight/flag superseded records.
- CONFIDENCE + RECENCY: composite decay (ClawMem-style half-lives).
- TRUST-TIER: `records` (primary) weighted above `summaries` (auto).

Keep all consumer semantics OUT of this repo (the seam from the epic).

## Build order (each a shippable slice)

1. Embedding backfill + `vss` HNSW index (resumable, incremental).
2. BM25 via `fts`; RRF fusion; `retrieve_candidates(query, k)`.
3. Cross-encoder rerank → `retrieve_relevant(query, ctx)` (is-565.2 done).
4. `supersessions` table + general link ingest; ranking composition (is-565.3).
5. Latency gate against the live 857k index; MCP tool + hook wiring.

Steps 1–3 pull in the ML dependency + the large backfill; that is the point
to confirm scope before starting.

## Decisions confirmed (2026-06-22, principal)

- **Engine: DuckDB-native** (`vss` HNSW + `fts` BM25). sqlite-vec rejected.
- **Models: ONNX via `fastembed`** acceptable (no torch). all-MiniLM-L6-v2
  (embed) + ms-marco-MiniLM-L-6-v2 (rerank).
- **Backfill: the whole corpus** (~857k records). The backfill MECHANISM is
  resumable/incremental (embed only new/changed records, tracked like
  `file_mtimes`); the actual full run is an OPERATIONAL step against the live
  DB, not something a unit test does — workers verify on a small fixture
  corpus, the full embed is run separately once the mechanism lands.
- **Execution: dispatched to TDD workers**, pipelined to avoid merge churn on
  the shared files (core.py / retrieval.py / server.py):
  - is-565.3 schema (supersessions table + link-ingest API) — IN FLIGHT.
  - is-565.2a embeddings + `vss` vector candidate search.
  - is-565.2b `fts` BM25 lexical candidates + RRF fusion → retrieve_candidates.
  - is-565.2c cross-encoder rerank → `retrieve_relevant` + MCP tool/hook.
  - is-565.3 ranking composition (recency×confidence×not-superseded×trust-tier)
    over 2a/2b/2c, consuming the supersessions table.
  Each lands on its own branch off the latest main; coordinator reviews +
  merges before dispatching the next dependent slice.
