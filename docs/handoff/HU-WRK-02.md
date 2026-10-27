# HU-WRK-02 — Low-stock alert job: handoff and evidence

Story: `code-corhuila/synkro-docs#265`. Repository: `synkro-worker`. Branch: `feat/implement-low-stock-alert-job`,
cut from `origin/develop` at `ea0bb32488f6db09c26877fad4dd27c9b2ef147f`. Evidence gathered on 2026-10-27 (UTC).

This file is the handoff for the central documentation work (DOCS-98). Nothing in `synkro-docs` or in any other
repository was edited by this story.

## 1. What was delivered

| Area | Where | Notes |
|---|---|---|
| Domain | `internal/domain/model/stock.go` | `Product`, `StockAlert`, `IsLowStock`, `OpeningIntent` with the `low-stock:<productId>:<date>` key (UTC date). Standard library only. |
| Ports | `internal/application/port/` | Output port `ProductsGateway` and its error classes; input port `LowStockAlertSync`; `Clock`; correlation id carried in `context`. |
| Use case | `internal/application/usecase/low_stock.go` | Open phase, resolve phase, run budget, cursors, failure accounting, circuit stop. |
| Products adapter | `internal/adapter/out/products/client.go` | HTTP client of the public Products contract: bounded parsing, retries, backoff with full jitter, no redirects. |
| Scheduler | `internal/adapter/in/scheduler/` | Per-run correlation id, panic recovery, `LowStockJob` adapter. |
| Configuration | `internal/config/config.go`, `.env.example` | Validation with explicit errors; new `LOW_STOCK_MAX_PAGES`. |
| Composition | `cmd/worker/main.go` | Real job replaces the placeholder. |

`deploy/compose.yml` and `deploy/Dockerfile` are unchanged: `SERVICE_TOKEN` is still injected from
`WORKER_SERVICE_TOKEN`, `PRODUCTS_API_URL` is unchanged, no port is published, and the production binary is
still built with `CGO_ENABLED=0` on Go 1.23.

## 2. Design decisions to record

1. **Budget.** `BATCH_SIZE` (1..100, the API page limit) is the page size. Each phase reads at most
   `LOW_STOCK_MAX_PAGES` pages (default 5), so a run reads at most `2 * MaxPages * BatchSize` list items plus one
   product read per examined alert not already read as low. ADR-007 Decision 6 says "up to `BATCH_SIZE` per run";
   `LOW_STOCK_MAX_PAGES=1` reproduces that literally. The ADR text was **not** edited; DOCS-98 should decide whether
   to align the ADR wording or keep the setting.
2. **Pagination progress.** Page cursors per phase live in memory only and wrap to page 1 after the last page.
   Resolving removes alerts from the `OPEN` list and shifts later pages, so a run reads all its alert pages first
   and resolves afterwards; across runs an item can be skipped until the next pass.
3. **Starvation.** The two phases have independent page budgets, and the open phase may use at most half of the
   time left before `RUN_TIMEOUT`.
4. **Idempotency key.** One key per intention, built once from the injected clock (UTC date) and re-sent unchanged
   by every HTTP retry. No retention or semantics beyond the contract were assumed.
5. **Retries.** Network errors, timeouts, `429` and `5xx` only; exponential backoff (base 200 ms, cap 5 s) with
   full jitter; `HTTP_ATTEMPTS` is the total number of attempts. `401/403` stop the whole run. `404` is never
   taken as replenishment.
6. **Configuration is stricter than before.** Invalid durations and integers used to fall back to the default
   silently (and integers could be negative). They now stop startup with an error naming the setting, never its
   value. Bounds: threshold `>= 0`; `BATCH_SIZE` 1..100; `LOW_STOCK_MAX_PAGES` 1..100; `HTTP_ATTEMPTS` 1..10;
   `LOW_STOCK_EVERY >= 1s`; `HTTP_TIMEOUT <= RUN_TIMEOUT`; URL must be http(s) without credentials/query.
7. **Panic and shutdown policy.** A panic or error ends only that run and is logged with the run's correlation id
   (the stack is logged for a panic). Shutdown cancels the run's context and waits for the job to return. The
   deadline stays cooperative.

## 3. Finding for Products / contract owners — D-1 (not fixed here)

**Case.** Same UTC day: stock drops (alert A opens with key `low-stock:P:D`), stock is replenished (A resolved),
stock drops again.

**Observed against real Products** (revision `8c04934`): `POST /api/v1/stock-alerts` with the same key answers
`200` with alert A, now `RESOLVED`. No new alert is opened, so product P has stock `<= threshold` and no `OPEN`
alert until the next UTC day (a different key). The cause is in `OpenOnce`: a repeated key returns "the alert it
produced, as it is now" before the partial unique index is consulted.

**What the Worker does.** It does not count that answer as an opening: the item is failed, the log line names the
product, the alert id, the key and the `RESOLVED` status. It does not invent another key format.

**Options for the owners** (not decided or implemented here): (a) Products opens a new alert when the replayed
alert is `RESOLVED`; (b) the contract defines a key with a component that changes per intention; (c) accept the
once-per-day limit and document it. Either needs a decision on the Products contract/ADR-007 and, for (a) or (b),
a change in `synkro-products-api` (separate PR in that repository).

## 4. Verification actually run

### 4.1 Local, Go 1.23.12 (Linux container `golang:1.23-bookworm`, repository mounted read-only)

| Command | Result |
|---|---|
| `go build ./...`, `go vet ./...` | pass |
| `go test -count=1 ./...` | pass (171 test runs including sub-tests) |
| `go test -race -count=5 ./...` | pass |
| domain import check (as in `ci.yml`) | domain imports only the standard library |

Also run on the developer machine (Windows, Go 1.27.0, no cgo, so no `-race`): `go vet ./...` and
`go test -count=1 ./...` pass. The production image (`deploy/Dockerfile`, unchanged) builds.

Red phase, kept in the author's scratch area: use-case tests failed against a no-op stub (26 failing, 1 hanging on
the overlap test); adapter tests failed with `not implemented` (26 failing); scheduler tests failed with
`undefined: LowStockJob`; config tests failed to build against the original `config.go` (`MaxPages` missing).

### 4.2 Real Products and real Auth (not fakes)

Environment: the local `synkro-dev` platform started from the candidate composition recorded in
`_local-guides/LOCAL-SYNKRO-ENVIRONMENT.json`: Products API `8c04934` (tag v2.0.0), Auth API `e3ea374`, Gateway
`5e5c3d5`, Infra root `8139a43` (PR #13 head), docs `61056c93`. The Worker that the platform runs
(`synkro-dev-synkro-worker-1`, built from `7827aa1`, placeholder job) was **not** modified or stopped and is what
remains running. The code under test ran in a separate container started from a local image
`synkro-worker:hu-wrk-02-local` (`sha256:52a6d82bb7521e264bd36e8d60cf6c2a037131af492cb72ea3780d6f6f6bf2d0`) built
from this working tree on top of `ea0bb32`, attached to the `platform` network, with the Worker's real SERVICE
token (claims: `sub=synkro-worker`, `roles=SERVICE`, `permissions=products:read,stock-alerts:read,stock-alerts:write`)
passed by environment variable name and never printed. Run settings: `LOW_STOCK_EVERY=5s`, `RUN_TIMEOUT=30s`,
threshold 5.

Fixtures, created and changed only through the Products API as `inventory@synkro.example.test`:

- namespace `HU-WRK-02-20261027T0312`; category `01a17b58-6548-7e16-a1db-16c9169cba56`
- products: `zero` (stock 0) `01a17b58-659f-7169-9dd7-b02dfe71980a`, `low` (3) `01a17b58-65fc-78d4-963c-972313e1ea1d`,
  `edge` (5) `01a17b58-666e-7502-8f34-277d1281d92e`, `above` (50) `01a17b58-66fa-7ac3-b0cf-84f3db7d7a1a`
- the pre-existing product `BATERIA` (stock 31) and category `CARGA` were not touched

| # | Scenario | Observed |
|---|---|---|
| 1 | First run | 3 products examined, 3 alerts opened (`zero`, `low`, `edge` with `stockAtOpening` 0, 3, 5), none for `above` |
| 2 | Repeat | next runs: `opened=0`, `alreadyOpen=3`; still 3 alerts |
| 3 | Replenish `low` (+20) | the next run resolved its alert (`RESOLVED`, `resolvedAt` set); later runs resolved nothing |
| 4 | Repeat resolution (Worker token, `POST .../resolve` again) | `200`, same alert, same `resolvedAt` |
| 5 | Authority | the INVENTORY user gets `403` on `POST .../resolve` |
| 6 | D-1 (same-day drop of `low`) | run failed that item: `200` with a `RESOLVED` alert for the daily key; no `OPEN` alert for `low` |
| 7 | Credential rejected (placeholder string as token) | `401`; exactly one request; run aborted; the `traceId` returned by Products equals the run's correlation id (propagation proven) |
| 8 | Products unreachable (unknown host) | 3 attempts per call, then failed; run ended in about 11 s; both phases attempted once |
| 9 | Pagination across runs (`BATCH_SIZE=1`, `LOW_STOCK_MAX_PAGES=1`) | 1 product and 1 alert examined per run, cycling through the 3 low products |
| 10 | `docker stop` | exit code 0 in about 0.4 s, log `synkro-worker stopped` |

Cleanup: `zero` and `edge` were restocked (+10), the Worker resolved their alerts, then the four fixture products
and the fixture category were deactivated through the API. Final state: 0 `OPEN` alerts. The three `RESOLVED`
alerts remain as history because the contract offers no delete. All test containers were removed; 17 containers of
the platform kept running.

### 4.3 Not exercised against real Products

`429` and `5xx` answers, a service token with only some of the three permissions (`403`), a catalog larger than
one page in a single run, and cancellation in the middle of a real request. These are covered only by the
controlled-HTTP and unit tests above, which are adapter/unit evidence, not integration evidence.

## 5. Handoff for DOCS-98 (to be done in `synkro-docs`, not here)

- **TD-019:** evidence that the Worker now has a real job against Products (sections 4.1 and 4.2).
- **TD-020:** implemented and verified here: per-run panic recovery, bounded retries with backoff and jitter,
  explicit configuration errors, cooperative cancellation with no new item after cancellation, no overlapping
  runs. Not implemented: hard termination of a job that ignores its context (deliberately not promised).
  Close TD-020 only for that scope.
- **FR-004 traceability / service catalog:** the Worker calls `GET /products?stockAtMost`, `GET /products/{id}`,
  `GET /stock-alerts?status=OPEN`, `POST /stock-alerts`, `POST /stock-alerts/{id}/resolve`.
- **Runbook:** settings table (`.env.example`), the log fields (`correlationId`, `processed`, `failed`, `opened`,
  `alreadyOpen`, `resolved`, `stillLow`) and the meaning of a failed item.
- **D-1** (section 3) and the `LOW_STOCK_MAX_PAGES` wording of ADR-007 Decision 6 need an owner decision.
