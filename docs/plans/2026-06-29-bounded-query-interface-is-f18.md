# Bounded query interface — replacing arbitrary `query(sql)` (is-f18)

Session: (design deliverable for pebble is-f18)
Status: PROPOSAL (design only; implementation tracked as follow-up pebbles below)
Cross-refs: is-189 (single DB-thread HOL blocking + statement_timeout + connect
config), is-fd7 (read-only connection, read/write tool split, fast-fail budgets).
This pebble is the **interface-design layer above both**.

## 0. Problem in one line

The MCP surface exposes `query(sql)` — arbitrary SQL on a read-write DuckDB
connection. A caller ran `records.raw ILIKE '%term%'` (a leading-wildcard,
btree-unservable scan over the ~83 GB `records` table); on the single
`_db_loop` thread that monopolized the one connection and starved every other
query, including a trivial `SELECT name FROM sqlite_master` (is-189). An
interface that *allows* a poor-performing query is the root defect. is-189's
statement_timeout bounds the blast radius; the fix here is to make the slow
query **unrepresentable** — the caller has no way to express a raw scan because
no method accepts SQL, a `WHERE` fragment, or a `LIKE` pattern.

Premise (from the pebble): the full set of query patterns we need is **closed
and known**. The historical mining below proves it. So the open-ended SQL
surface buys nothing and costs correctness.

---

## 1. Consumer inventory — every caller + its actual access pattern

Two surfaces exist: the **MCP tools** (`@server.call_tool`) and the **REST
endpoints** (`/api/*`, consumed by the hooks). Both funnel through
`_db_execute` onto the single DB thread. Internal `*.py` callers reach the DB
directly on that thread.

### 1a. MCP tools (`server.py:list_tools` / `call_tool`)

| Tool | Access pattern | Index/path it actually needs |
|------|----------------|------------------------------|
| `query(sql)` | **Arbitrary SQL.** The defect. Used for schema discovery, lexical content search, project/branch scoping, get-by-id, counts. See §3 mining. | — (to be removed/gated) |
| `get_session(session_id, include_blobs, format)` | Full transcript by id, chronological, blobs rehydrated | `records` PK + `idx_records_session_id`; `read_blob` macro |
| `retrieve_relevant(query, k, candidate_k)` | Fused vector+BM25 candidates → cross-encoder rerank → is-565.3 ranking | HNSW + `match_bm25` + ranking compose |
| `context(session_id)` | Assemble LCM recovery context | `idx_summaries_session_id` (+ dag reads) |
| `summarize(session_id)` | **Write**: DAG maintenance (sprigs/bindles) | summaries writes; LLM off-thread |
| `refresh(path)` | **Write**: ingest one JSONL | records/sessions writes |
| `add_supersession(superseding_id, superseded_id, source)` | **Write**: one supersession link | `supersessions` upsert |
| `backfill(batch_size, wait)` | **Write/admin**: embed unembedded + rebuild FTS | anti-join + HNSW + FTS rebuild |
| `sync(batch_size, wait, force_fts)` | **Write/admin**: anti-join embed sweep + maybe FTS | same as backfill |
| `schema` resource (`ingest-sessions://schema`) | Read `information_schema` tables/columns | catalog (typed reader makes this redundant — §4) |

### 1b. REST endpoints (`run_http`) — the hook-facing surface

| Endpoint | Caller (hook) | Maps to |
|----------|---------------|---------|
| `POST /api/context` | `hooks/session_start.py` (SessionStart) | `get_latest_summarized_session` then `assemble_context_for_session` |
| `POST /api/retrieve` | `hooks/_retrieve_common._retrieve` (used by `session_retrieve.py` SessionStart + `first_prompt_retrieve.py` UserPromptSubmit) | `retrieve_relevant(query, k=5, candidate_k=20)` |
| `POST /api/refresh` | `hooks/pre_compact.py` (PreCompact step 1) | `_ingest_file_full` (**write**) |
| `POST /api/summarize` | `hooks/pre_compact.py` (PreCompact step 2) | `_run_summarize_async` (**write**) |
| `POST /api/session` | full-session fetch (is-565.1) | `_get_session_payload` |
| `POST /api/supersessions` | `scripts/supersession_adapter.py` (external git-revert / ADR-amend detector) | `add_supersessions` (**write**, batch) |
| `POST /api/backfill`, `POST /api/sync` | operational one-shots / tests | embed/FTS sweeps (**write/admin**) |

**Finding:** every REST consumer is already purpose-built and typed. **None of
the hooks use `query(sql)`.** The arbitrary-SQL surface is used only by
*interactive agents* doing ad-hoc corpus mining — exactly the population the
mining in §3 characterizes.

### 1c. Internal programmatic callers (direct DB-thread functions)

These already ARE the typed, index-backed methods the proposal generalizes:

- `retrieval.search_lexical` → `match_bm25` over `record_fts` (the correct
  text-search path; LIKE bypasses it).
- `embeddings.search_vector` / `search_summaries` → HNSW cosine.
- `retrieval.get_full_session` → `idx_records_session_id`, ordered, blob rehydrate.
- `dag.get_unsummarized_messages` / `get_sprigs_for_session` /
  `get_latest_bindle_content` / `assemble_context_for_session` →
  `idx_summaries_session_id`, `idx_summaries_kind`.
- `dag.get_latest_summarized_session` → `sessions ⋈ summaries`; **note** it uses
  `project_path LIKE ? || '%'` (a *trailing*-wildcard prefix, index-servable)
  and a `file_path LIKE '%/' || session_id || '.jsonl'` fallback (a leading
  wildcard over the *small* `file_mtimes` table — acceptable, but flagged).
- `supersession.get_superseded_ids` / `get_superseded_session_ids` /
  `is_superseded` / `get_supersessions_for` → `idx_supersessions_superseded_id`.
- `blobs.read_blob` / `get_blob_meta` → blob store + `blob_meta` PK.
- `core.*` ingest/FTS-rebuild → writer path.

**Total consumers inventoried: 26** — 10 MCP tools/resource, 8 REST endpoints,
and 8 internal typed-function families (the methods the public API will expose).

---

## 2. The existing index surface (what the API must map onto)

From `core.create_tables` (verified present in the installed server per is-189):

| Table | PK | Secondary indexes | Text/vector |
|-------|----|--------------------|-------------|
| `records` | `uuid` | `idx_records_session_id`, `idx_records_type`, `idx_records_timestamp` | — |
| `record_fts(uuid, text)` | `uuid` | `fts_main_record_fts.match_bm25(uuid, q)` (BM25 snapshot) | lexical |
| `record_embeddings` | `uuid` | `idx_record_embeddings_hnsw` (HNSW cosine) | vector |
| `summaries` | `summary_id` | `idx_summaries_session_id`, `idx_summaries_kind` | — |
| `summary_embeddings` | `summary_id` | `idx_summary_embeddings_hnsw` (HNSW cosine) | vector |
| `sessions` | `session_id` | **none** | — |
| `supersessions` | `(superseding_id, superseded_id)` | `idx_supersessions_superseded_id` | — |
| `blob_meta` | `file_id` | `idx_blob_meta_session_id` | — |
| `history` | `(timestamp, session_id)` | `idx_history_session_id` | — |

**Two index gaps the historical demand (§3) exposes:**

- **G1 — no FTS over `summaries.content`.** Sessions repeatedly ran
  `summaries.content LIKE '%...%'` (full scan). A bounded lexical-summary
  search needs either an FTS index over `summaries` or to accept a full scan of
  the *small* summaries table (it is orders of magnitude smaller than
  `records`). Recommend a `summary_fts` index for symmetry with `record_fts`.
- **G2 — no index on `sessions.project_path` / `sessions.git_branch`.**
  Historical project/branch scoping used `project_path LIKE '%mtm%'` (substring,
  unservable). The bounded `list_sessions` must constrain project/branch to
  **prefix or exact match** (index-servable with a new btree) — not arbitrary
  substring.

---

## 3. Historical raw-SQL demand — mined from cited sessions

Extracted directly from the session JSONL of the pebble-cited sessions
(`6262da9e`, `552de17d`, `cea64cd5`, …) — the actual `sql` arguments those
agents passed to `query()`. The `/api/retrieve` corroboration surfaced exactly
the cited sessions (`ee52eb5d`, `0b5908a4`, `cea64cd5`) tagged
"schema-discovery / DB-mining". The demand collapses to **six recurring
shapes**, and the same two failures repeat every time: **(F1)
schema-rediscovery-by-trial-and-error** and **(F2) un-indexed leading-wildcard
LIKE that full-scans `records`**.

### Shape A — schema rediscovery (dominant waste; F1). Every session.
```sql
SELECT name FROM sqlite_master WHERE type='table';
SELECT sql FROM sqlite_master WHERE type='table' AND name IN ('records','sessions','summaries');
SELECT column_name FROM information_schema.columns WHERE table_name='records';
```
→ **Eliminated entirely** by a typed API: the method signatures ARE the schema.
(Sessions still guessed wrong after running these — expecting `id`/`created_at`
when it is `session_id`/`modified`.)

### Shape B — lexical content search + per-session hit ranking (the workhorse; F2)
```sql
SELECT session_id, count(*) AS hits, min(timestamp) AS first_ts
FROM records WHERE raw ILIKE '%exophial%'
  AND (raw ILIKE '%pb ready%' OR raw ILIKE '%dispatch%' OR raw ILIKE '%autonomous%')
GROUP BY session_id ...;                              -- 6262da9e (oracle/exophial hunt)

SELECT ... FROM records WHERE raw ILIKE '%"role":"user"%'
  AND raw NOT ILIKE '%tool_result%' AND raw ILIKE '%exophial%' ...; -- role filter + exclude noise

SELECT session_id, count(*) AS hits, min(timestamp), max(timestamp)
FROM records WHERE lower(CAST(raw AS VARCHAR)) LIKE '%coverage%'
  AND (... LIKE '%arcade%' OR ...) GROUP BY session_id;            -- cea64cd5 (coverage hunt)
```
→ **The is-189 killer.** Maps to `search_records(text, …, group_by_session)` over
`match_bm25` — indexed, bounded, with a `role` / `exclude_tool_results` filter
and per-session hit aggregation.

### Shape C — lexical search over **summaries** (G1)
```sql
SELECT ... FROM summaries s WHERE s.content LIKE '%multilang-support%'
  AND (s.content LIKE '%merge%' OR s.content LIKE '%promote%' ...);  -- 552de17d
SELECT ... FROM summaries WHERE lower(content) LIKE '%code coverage%' ...; -- cea64cd5
```
→ `search_summaries_lexical(text, …)` over a new `summary_fts` (or bounded scan
of the small summaries table).

### Shape D — project / branch / time scoping (G2)
```sql
SELECT session_id, modified, git_branch, message_count, substr(summary,1,100)
FROM sessions WHERE project_path LIKE '%aspergillus%' OR git_branch='multilang-support';
SELECT session_id, modified, git_branch, substr(first_prompt,1,200)
FROM sessions WHERE project_path LIKE '%aspergillus%' ORDER BY modified DESC LIMIT 15;
```
→ `list_sessions(project=, branch=, since=, order='modified', limit)` — typed
filters on (newly-indexed) `project_path` prefix / `git_branch` equality.

### Shape E — get-by-id + time-range + field extraction
```sql
SELECT timestamp, type, json_extract_string(raw,'$.message.content')
FROM records WHERE session_id='1ae535bb-…'
  AND timestamp BETWEEN '2026-06-12 22:55:00' AND '2026-06-12 23:30:00'
  AND type IN ('user','assistant');                                 -- cea64cd5
```
→ `get_session_records(session_id, type=, since=, until=, limit)` —
`idx_records_session_id` + typed `type`/timestamp filters. Field extraction
(`$.message.content`) becomes a structured field on the returned record.

### Shape F — corpus counts / stats
```sql
SELECT DISTINCT project_path, count(*) AS n FROM sessions GROUP BY project_path ORDER BY n DESC LIMIT 20;
```
→ `corpus_stats()` — a bounded, purpose-built aggregate (project distribution,
row counts, embed/FTS coverage).

**Conclusion:** zero historical queries need arbitrary SQL. Every one maps onto
a small typed method. F1 vanishes (the signature is the schema); F2 vanishes
(text only ever reaches `match_bm25`, never `LIKE`).

---

## 4. Proposed bounded interface

A single read-only facade, `SessionReader` (wrapping the existing pure
`retrieval`/`embeddings`/`dag`/`supersession`/`blobs` functions), exposed as a
**closed set of MCP tools** that replace `query`. Companion writer tools stay as
they are (already typed). Design rules, all enforced at the signature:

- **No `sql`, no `where`, no `like`, no `order_by` string parameters.** Text
  search is a `text: str` value bound ONLY into `match_bm25(uuid, ?)`. Filters
  are typed scalars/enums bound only to equality/range/prefix predicates.
- **Mandatory `limit: int`** on every list/search method, hard-capped
  (e.g. `limit = min(limit, MAX_LIMIT)`).
- **Pagination by typed keyset** (`after_uuid` / `after_ts`), never `OFFSET`
  over a scan.

### Reader methods (index-backed)

| Method | Replaces (shape) | Index served by |
|--------|------------------|-----------------|
| `get_session(session_id, *, include_blobs, format)` | get_session tool | `idx_records_session_id` + blob macro |
| `get_session_records(session_id, *, type=None, since=None, until=None, limit)` | Shape E | `idx_records_session_id` (+ typed type/ts filter) |
| `search_records(text, *, project=None, branch=None, role=None, exclude_tool_results=True, since=None, until=None, limit)` | Shape B | `match_bm25` (FTS) + post-filter on indexed cols |
| `search_records_by_session(text, *, …same filters, top_sessions, limit)` | Shape B (the `GROUP BY session_id count(*)` rank) | `match_bm25` → aggregate hits per `session_id` |
| `search_summaries_lexical(text, *, kind=None, limit)` | Shape C | `summary_fts` (G1) or bounded summaries scan |
| `retrieve_relevant(query, *, k, candidate_k)` | retrieve tool/endpoint | HNSW + BM25 + rerank + ranking (unchanged) |
| `list_sessions(*, project=None, branch=None, since=None, until=None, order='modified', limit)` | Shape D | new `sessions(project_path)` / `(git_branch)` btree (G2) |
| `get_summaries(session_id, *, kind=None)` | dag/context reads | `idx_summaries_session_id`, `idx_summaries_kind` |
| `get_context(session_id)` | context tool/endpoint | dag composition (unchanged) |
| `read_blob(file_id)` | blob rehydrate | `blob_meta` PK + blob store |
| `supersessions_for(record_id)` / `is_superseded(record_id)` | supersession reads | `idx_supersessions_superseded_id` |
| `corpus_stats()` | Shape F | bounded aggregate over indexed cols |

### Writer/admin methods (separate surface — composes with is-fd7)

`refresh`, `summarize`, `add_supersession(s)`, `backfill`, `sync` — unchanged in
behavior, but moved behind a writer connection per is-fd7 item 1/3. The reader
methods above run on the read-only connection; a reader tool **cannot mutate by
construction** (it holds no write handle).

### The escape hatch (non-default, gated — for ad-hoc ops only)

If an ad-hoc analytical query is genuinely needed, expose `raw_sql(sql)` ONLY:
- on a **read-only** connection (`duckdb.connect(path, read_only=True)`; rejects
  DDL/DML by construction — is-fd7 item 1),
- under a **`statement_timeout`** so a runaway scan is killed, not queued forever
  (is-189 item 1),
- with an **`EXPLAIN`-reject of `SEQ_SCAN` over `records` / `record_fts` /
  `blob_meta`** before execution (fail-closed),
- **clearly marked non-default** (disabled unless `INGEST_SESSIONS_ENABLE_RAW_SQL=1`),
  documented as "ops-only, not for agents."

This hatch is **by-verification**, not by-construction — see §5.

---

## 5. Correct-by-construction — the three-question test (codebase.md)

The pebble asks for correct-by-construction framing. Applying the test honestly,
the guarantee **splits in two**, and only the first half earns the term.

**Claim 1 (BY CONSTRUCTION):** *A caller cannot express a raw-SQL / arbitrary-
`WHERE` / `LIKE` query through the reader API.*
1. **Invariant:** no reader method accepts SQL text, a predicate fragment, or a
   wildcard pattern; the only free-text input is `text: str`, and filters are
   typed scalars/enums.
2. **Why unrepresentable:** there is **no parameter of SQL/predicate type** in
   any reader signature. A `%term%`-against-`records.raw` scan has no
   representation because no method's type can carry it — the only text path is
   `match_bm25`. This is *static*: the type of the API excludes it, exactly like
   the wire-contract precedent (a dropped field fails to type-check). Removing
   `query(sql)` from the tool list makes arbitrary SQL **unrepresentable on the
   MCP surface**, not merely rejected.
3. **More than a check?** Yes — there is no guard to evaluate at runtime because
   there is no input to guard. **By construction.**

**Claim 2 (BY VERIFICATION — downgraded wording):** *Each reader method is
index-served (no full/leading-wildcard scan).*
1. **Invariant:** every reader method's SQL touches an index (PK, secondary
   btree, FTS, or HNSW).
2. **Why:** that `search_records` routes `text` to `match_bm25` and not to
   `LIKE`, and that `list_sessions` filters hit the new `project_path` btree, is
   a property of the **implementation**, not the signature. A future edit could
   reintroduce a `LIKE` inside a method without changing its type.
3. **More than a check?** No. This is **verified by an EXPLAIN-plan test in CI**
   that asserts no `SEQ_SCAN` over `records`/`record_fts`/`blob_meta` for any
   reader method (and by code review). So: **"verified by the EXPLAIN-plan
   gate,"** NOT "by construction."

**The escape hatch** (`raw_sql`) is entirely **by-verification**: read-only
connection (construction-ish: no write handle), `statement_timeout`, and a
fail-closed `EXPLAIN`-reject. Per fail-fast doctrine these are bounded guards on
a deliberately non-default, ops-only path — not a fallback chain on the default
path, because the default reader path carries Claim 1's structural guarantee.

**How f18 / 189 / fd7 compose (one property each, not stacked guards on one
property):**
- **f18 (this doc):** removes the footgun from the *type* — slow query
  unrepresentable on the default surface (by construction).
- **fd7:** the reader connection is *read-only* and each tool has a *bounded
  budget* — mutation + unbounded-hang removed (by construction / verification).
- **189:** `statement_timeout` + `memory_limit`/`threads` connect config bound
  the *residual* DB-thread op (the escape hatch, heavy writes) so it can't
  starve the queue (verification).

---

## 6. Migration note — per current caller

| Current caller | Today | After |
|----------------|-------|-------|
| Interactive agent: schema discovery (Shape A) | `query("SELECT … sqlite_master/information_schema")` | **Deleted** — method signatures + tool docs are the schema; `corpus_stats()` for counts |
| Interactive agent: content hunt (Shape B) | `query("… raw ILIKE '%x%' GROUP BY session_id")` | `search_records_by_session(text=…, role=…, exclude_tool_results=True, limit=…)` |
| Interactive agent: summaries hunt (Shape C) | `query("… summaries.content LIKE '%x%'")` | `search_summaries_lexical(text=…, kind=…, limit=…)` |
| Interactive agent: project/branch scope (Shape D) | `query("… sessions WHERE project_path LIKE '%x%'")` | `list_sessions(project=…, branch=…, since=…, order='modified', limit=…)` |
| Interactive agent: by-id + time + field (Shape E) | `query("… WHERE session_id=… AND timestamp BETWEEN … AND type IN …")` | `get_session_records(session_id, type=…, since=…, until=…, limit=…)` |
| Interactive agent: corpus stats (Shape F) | `query("… GROUP BY project_path")` | `corpus_stats()` |
| `get_session` tool / `/api/session` | unchanged behavior | `get_session(...)` (same) |
| `/api/retrieve` (3 hooks) | `retrieve_relevant` | **No change** (already typed/index-backed) |
| `/api/context` (`session_start.py`) | `assemble_context_for_session` | `get_context(...)` (rename only) |
| `/api/refresh`, `/api/summarize` (`pre_compact.py`) | writer endpoints | **No change** (writer surface, is-fd7) |
| `/api/supersessions` (`supersession_adapter.py`) | `add_supersessions` batch | **No change** (writer surface) |
| `backfill` / `sync` tools+endpoints | admin sweeps | **No change** (writer/admin surface) |
| `schema` MCP resource | dynamic catalog dump | **Deprecate** — superseded by typed signatures (keep read-only for one release, then remove) |
| `dag.get_latest_summarized_session` internal | `project_path LIKE ?||'%'` (prefix, OK) + `file_path LIKE '%/'||sid||'.jsonl'` fallback | Keep; the fallback's leading wildcard is over the *small* `file_mtimes` table — acceptable, but fold the project-prefix path onto the G2 index when added |

No hook or external caller loses a capability: the hooks never used `query`, and
every interactive use maps onto a typed method. The only removed capability is
**ad-hoc arbitrary SQL**, preserved (gated, non-default) via the §4 escape hatch.

---

## 7. Proposed follow-up implementation pebbles

1. **is-f18.1 — reader facade + tool surface.** Implement `SessionReader`
   (read-only connection per is-fd7) with the §4 reader methods; replace the
   `query` tool with `search_records`, `search_records_by_session`,
   `list_sessions`, `get_session_records`, `corpus_stats`; keep
   `get_session` / `retrieve_relevant` / `context`. Acceptance: an EXPLAIN-plan
   test (Claim 2 gate) asserting no `SEQ_SCAN` over `records`/`record_fts`/
   `blob_meta` for any reader method, plus the `query` tool gone from
   `list_tools`.
2. **is-f18.2 — index gaps G1/G2.** Add `summary_fts` (BM25 over
   `summaries.content`) and btree indexes on `sessions(project_path)` /
   `sessions(git_branch)`; wire `search_summaries_lexical` + `list_sessions`
   filters to them.
3. **is-f18.3 — gated escape hatch.** `raw_sql` on a read-only connection +
   `statement_timeout` + `EXPLAIN`-reject of seq-scans, behind
   `INGEST_SESSIONS_ENABLE_RAW_SQL` (default off). Depends on is-fd7 item 1
   (read-only reader connection) and is-189 item 1 (statement_timeout).
4. **is-f18.4 — deprecate the `schema` resource** once is-f18.1's typed surface
   ships (one-release deprecation, then remove).

Sequence: f18.2 (indexes) → f18.1 (facade, needs the indexes) → f18.3/f18.4.
is-fd7 (read-only + writer split) and is-189 (statement_timeout + connect
config) land independently and are dependencies of f18.3.
</content>
