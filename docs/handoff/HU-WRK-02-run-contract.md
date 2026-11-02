# HU-WRK-02 — Run-scoped alert reconciliation: handoff and evidence

Story: `code-corhuila/synkro-docs#265` (HU-WRK-02, still open). Decision: ADR-018, Accepted 2026-10-31
(HU-ARQ-29 / `code-corhuila/synkro-docs#287`), which modifies only the opening `Idempotency-Key` clause of
ADR-007 Decision 6. Requirement: FR-004. Repository: `synkro-worker`, branch `fix/complete-low-stock-run-contract`,
cut from `origin/develop` at `431737a`. Evidence gathered on 2026-10-31 (America/Bogota).

This is a continuation of the story, not a new one. [`HU-WRK-02.md`](HU-WRK-02.md) stays as the historical
evidence of 2026-10-27. Nothing in `synkro-docs`, Products, Infra or any other repository was edited. This file
claims no closure of the story, no promotion to `qa` or `main`, no new QA result and no tag.

## 1. Context

The job (PR #14 on `develop`, PR #16 on `qa`) built its key from the UTC date. After ADR-018 that key is wrong:
if an alert opens and is resolved and the stock drops again the same day, the key is replayed and Products returns
the `RESOLVED` alert, so no new `OPEN` alert exists. Three gaps are corrected here:

1. the key and the run identity (ADR-018);
2. a valid `RESOLVED` answer to an opening, which was classified as a malformed response;
3. the work budget, which was applied per phase although ADR-007 says "up to `BATCH_SIZE` per run" (not ratified
   by ADR-018, handled here on its own merits).

## 2. Files

| Area | File | Change |
|---|---|---|
| Domain | `internal/domain/model/stock.go` | `NewOpeningIntent(productID, stock, runID)` builds `low-stock:<productId>:<runId>`; `ValidateRunID`, `ErrInvalidRunID`. Standard library only. |
| Port | `internal/application/port/sync.go` | `Clock` removed; `SyncReport.ReconciliationPending`; run identity documented on `WithCorrelationID`. |
| Port | `internal/application/port/products.go` | `OpenAlertResult` documents that 200/201 do not say the alert is `OPEN`. |
| Use case | `internal/application/usecase/low_stock.go` | Identity check before any request; `Config.BatchSize`; `phaseQuotas`; element budget; cursors; `RESOLVED` handling. No clock. |
| Scheduler | `internal/adapter/in/scheduler/scheduler.go` | Generator returns an error (no timestamp fallback); run does not start without a valid UUID. |
| Scheduler | `internal/adapter/in/scheduler/lowstock_job.go` | Logs and reports `reconciliationPending`; such a run is not clean. |
| Composition | `cmd/worker/main.go` | `systemClock` removed; `BatchSize`. |
| Tests | `internal/**/**_test.go` | See section 5. |
| Acceptance | `internal/acceptance/lowstock_real_test.go` | Opt-in, build tag `acceptance`. |
| Docs | `README.md`, `.env.example`, this file, `HU-WRK-02.md` (notice only), `docs/evidence/hu-wrk-02-run-contract/` | |

`go.mod`, dependencies, CODEOWNERS, the workflow, templates and `deploy/` are unchanged. The Products adapter
`internal/adapter/out/products/client.go` is unchanged: it already accepted a valid `RESOLVED` body with `200`
and `201` (`Created` only reflects the status) and already re-sent the same key and body on every attempt; its new
tests are regression guards.

## 3. Current contract

**Identity.** The scheduler generates one UUIDv4 (`crypto/rand`) for each run that actually starts and puts it in
the context as the correlation id. The same value is `X-Correlation-Id` and the `<runId>` of every key of that
run. If generation fails or the value is not a non-nil canonical UUID, the run does not start: an error without
secrets is logged, the job is not called, nothing is mutated, and the next tick recovers. The use case validates
the identity again before any request, so a direct call without it also makes none. No other UUID is generated
per product. No journal of runs or intentions exists; a restart is a new identity.

**Intention.** At most one per product per run (`seen` map, also across duplicate pages). It is built once, before
the first `POST`, from the first observation of the run; retries (the adapter's) send the same key, `productId`
and `stockAtOpening`, whatever happens to the stock or the day afterwards.

**`RESOLVED` answer.** A valid body with status `RESOLVED` and `200` or `201` increments
`SyncReport.ReconciliationPending` and nothing else: not `Opened`, not `AlreadyOpen`, not `Processed`. Decision:
it is **not** added to `Failed`, because the answer was valid and neither transport nor schema failed; `Failed`
keeps meaning an unconfirmed item. It is distinguishable in the report, the use-case warning
("opening reconciliation pending") and the job log (`reconciliationPending`), and `LowStockJob` returns an error for
such a run so that the scheduler does not log it as a success. No other key is tried in the same run. Other
statuses, another product and missing fields stay malformed.

**Budget.** See section 4.

## 4. Budget: unit and bounds

Unit: **reconciliation elements** returned by the product and alert lists and selected for evaluation in a run,
duplicates included. `BATCH_SIZE` is the TOTAL of both phases.

| `BATCH_SIZE` | Open quota | Resolve quota |
|---|---|---|
| 1 | 1 on even runs, 0 on odd runs | 0 on even runs, 1 on odd runs |
| `n >= 2` | `ceil(n/2)` | `floor(n/2)` |

- Open + resolve never exceeds `BATCH_SIZE`; an odd value gives the extra element to the open phase.
- Each phase keeps `limit = ceil(quota / LOW_STOCK_MAX_PAGES)` fixed across pages and runs. It reserves a
  full page of slots before requesting it, including when the response is short, and stops before a request
  would exceed the remaining slots. Unused slots are not reclaimed. With defaults, each phase can request
  five pages of ten rows: at most 100 requested rows across both phases. Contract-compliant returned rows
  cannot exceed that admission budget; duplicate rows consume slots as well. No page is truncated after reading.
- A quota is not lent. A full list in one phase cannot starve the other; with `BATCH_SIZE=1` both phases
  advance on alternate runs (the alternation counts started runs and restarts with the process).
- `LOW_STOCK_MAX_PAGES` caps list requests and determines their stable width. It never multiplies the total;
  `1` alone does not fix a per-phase budget. A response larger than its requested limit is malformed.
- Beyond the element budget, per selected element: an open element is one `POST`; a resolve element is at most one
  `GET /products/{id}` (skipped when the product was read as low in the same run) plus one `POST .../resolve`.
  Each request makes at most `HTTP_ATTEMPTS` attempts and the run is bounded by `RUN_TIMEOUT`. List reads add at
  most `LOW_STOCK_MAX_PAGES` per phase. So the element budget is **not** a cap on HTTP requests; the request
  count is bounded by quota, attempts and timeout together.
- Cursors: the open cursor moves past a page when its selected elements were evaluated and wraps after the last
  page. The `OPEN` alert portion is collected before any mutation; the alert cursor stays on the first page of the
  portion if something was resolved (the list shrank), advances if nothing was, wraps after the last page, and
  does not move if the run was cancelled before the portion was evaluated. An open phase interrupted mid-page keeps
  its cursor on that page. Cancellation and timeout stop the run as before; no overlap and per-run panic recovery
  are unchanged.

## 5. Tests (TDD)

Red first: tests were written before production code and failed to build against it
([`red-phase.txt`](../evidence/hu-wrk-02-run-contract/red-phase.txt)). The products-adapter tests added for the
`RESOLVED` body and the lost response passed on the unchanged adapter, which is why they are listed as regression
guards, not as red tests. Initial author green: [`green-phase.txt`](../evidence/hu-wrk-02-run-contract/green-phase.txt), before the review correction below.

Old tests were adapted to the corrected contract keeping their functional cases: the clock and the daily key
were removed, the daily-key replay test became the run-scoped tests below, the "RESOLVED is malformed" case became
an unexpected-status case, and the page-size cases became quota cases. No assertion was relaxed to turn a test green.

| Area | Tests (names abbreviated) |
|---|---|
| Domain | key shape and length (83 for canonical UUIDs); stable per run, distinct across runs; invalid identities (empty, blank, timestamp, nil, braces, URN, trailing newline, `:` separators); error does not echo the value |
| Scheduler | generator failure never calls the job and the next tick recovers; invalid generated identity; exactly one identity per run; UUIDv4 shape and uniqueness; short/failed random source is an error; `reconciliationPending` is not hidden |
| Use case | identity absent/invalid makes zero requests and a later run recovers; batch-1 alternation not consumed by an invalid run; new run keeps the OPEN alert and its stock; reopening after resolution; frozen intention across retries; restart; `RESOLVED` with 200/201; quotas table; both phases full; odd batch; batch 1 and 2; limit stability; page progress and wrap; alert cursor without skips; cancellation keeps cursors; short pages; `MaxPages` cap; duplicates spend budget; oversized page; one point-read and one mutation per resolve element |
| Adapter (controlled HTTP) | valid `RESOLVED` with 200 and 201; foreign product or bad fields stay malformed; lost response retried with identical key and body |

Commands and results are in `green-phase.txt`: `gofmt`, `go vet ./...`, `go vet -tags acceptance ./...`,
`go build ./...`, `go test -count=1 ./...` pass (117 top-level tests, 241 with sub-tests). Go on this machine is
1.27.0 on Windows (`go.mod` declares 1.23); `CGO_ENABLED=0` and there is no C compiler, so `go test -race` was
**not run** in that native author execution. It used no Docker.

Reviewer checks supersede the final-status role of that initial green record: Go 1.23.12 on Linux passed
format, both vet commands, build and **246 tests/subtests with the race detector**, using a network-disabled
container and a read-only public-source snapshot. See [review-go123.json](../evidence/hu-wrk-02-run-contract/review-go123.json)
and [source identity/counts](../evidence/hu-wrk-02-run-contract/review-source.json).

The reviewer reproduced a missing admission check with short-then-full pages: 102 rows could be received with
`BATCH_SIZE=100`, despite selecting only 100. [The failing regression](../evidence/hu-wrk-02-run-contract/review-budget-red.txt)
preceded the correction. Fixed-width requests now reserve slots before reading and never truncate a returned
page to fit. Existing cursor/cancellation tests retain their original two-item-page scenarios; the new regression
covers requested and received row counts for batches 6, 13, 14 and 100. The race-enabled final suite passed it.

## 6. Scenario map

"Controlled" means in-memory fakes or a controlled `httptest` server; they prove the Worker's behaviour, not
Products' index or transactions. "Real" means executed against a real Products and PostgreSQL.

| ID | Controlled (executed, green) | Real Products + PostgreSQL |
|---|---|---|
| WRK-KEY-01 | `TestOpenAlert_ALostResponseIsRetried…` (HTTP); `TestRun_TheIntentionIsFrozen…` (use case) | NOT EXECUTED |
| WRK-KEY-02 | `TestRun_ANewRunReusesTheOpenAlert…` | PASS: real client/use case with fixture-scoped list; repeated run retains one OPEN alert |
| WRK-KEY-03 | `TestRun_ALowStockAfterAnObservedResolution…` | PASS: API-prepared replenishment/resolution; real use case opens distinct A2 the same day |
| WRK-KEY-04 | `TestRun_AReplayOfAnOldKey…` (fake mimics the owner rule) and the `RESOLVED` tests | PASS: real client replays the old key and returns A1 RESOLVED while A2 stays OPEN |
| WRK-KEY-05 | `TestRun_ARestartedWorker…` | PASS: fresh use-case instance and run UUID; no full Worker process restart tested |
| WRK-KEY-06 | none: a fake cannot prove the index | PASS: eight distinct concurrent keys through real Products and PostgreSQL produce one OPEN alert |
| WRK-KEY-07 | duplicate-page, changing-observation and frozen-intention tests | NOT EXECUTED |
| WRK-KEY-08 | scheduler and use-case tests, zero requests asserted | not applicable (no Products involved) |
| WRK-KEY-09 | `TestRun_AValidResolvedAnswer…With200And201`, adapter 200/201 test | NOT EXECUTED (the race window is the owner's; not forced here) |

## 7. Real acceptance: reviewer execution

The author's initial checks had no fixtures and skipped these tests. The reviewer subsequently provisioned a
dedicated disposable PostgreSQL instance, applied the 17 migrations from Products DB
`043e485d16018fc4fb4cd60694c2a521c143ddbd`, and started real Products from published source
`8c04934ae5b51417a0b88506cc59734ef7913e64` (native Go 1.27.0). All three opt-in tests passed, with no skips.
See [actual execution](../evidence/hu-wrk-02-run-contract/review-real-products.json).

Authentication used disposable test-issued RS256 JWTs validated by real Products, not tokens issued by a running
Auth service. These results cover the real Worker use case and HTTP adapter through fixture-scoped lists; they
do not verify real catalog pagination, Worker-driven replenishment resolution, the production scheduler process,
real Auth issuance or the full root composition. A1's replenishment and resolution were prepared through public
APIs before the reopening test. Controlled tests retain the other boundaries in section 6.

The reviewer checked exact ownership labels before deleting test containers, volume and network, terminated only
its native Products process, and removed temporary credentials. Shared workstation services were preserved.
[Cleanup record](../evidence/hu-wrk-02-run-contract/review-fixture-cleanup.json).

Requirements to run them (token only in the environment, never in arguments or logs):

```bash
TEST_PRODUCTS_URL=<origin of Products> \
TEST_SERVICE_TOKEN=<SERVICE token with the three Worker permissions> \
TEST_LOW_PRODUCT_ID=... TEST_REOPEN_PRODUCT_ID=... TEST_REOPEN_A1_ID=... TEST_REOPEN_A1_KEY=... \
TEST_CONCURRENCY_PRODUCT_ID=... \
go test -tags acceptance -count=1 -v ./internal/acceptance/
```

| Variable | Fixture, prepared beforehand through the Products API by someone with inventory authority |
|---|---|
| `TEST_LOW_PRODUCT_ID` | isolated product with stock `<= 5` |
| `TEST_REOPEN_PRODUCT_ID`, `TEST_REOPEN_A1_ID`, `TEST_REOPEN_A1_KEY` | product whose alert A1 was opened with a run-scoped key (`TEST_REOPEN_A1_KEY`), then restocked above the threshold and resolved, then lowered to `<= 5`: no `OPEN` alert |
| `TEST_CONCURRENCY_PRODUCT_ID` | isolated product with stock `<= 5` and no `OPEN` alert |

The Worker's token cannot change stock, so the fixtures cannot be prepared by the tests. The tests create and
delete no product, use no SQL, never print the token, and run the real use case through a wrapper that limits it to
the fixture ids so that it cannot touch the shared catalog. Each scenario consumes its fixture (alerts are created
and the first one is not removed), so use fresh fixtures per execution.

## 8. Limits and what remains

- Full-system acceptance with real Auth issuance, the production Worker scheduler, its actual list/resolve path and independent team reproduction remains pending; section 7 records only the executed subset.
- Not delivered here: review, promotion to `qa`/`main`, tag, close of the story. Branch/PR flow belongs to the
  maintainers.
- ADR-018 consequences stay as accepted: one key mapping per visited low-stock product per run, no late-execution
  fencing, no atomic stock observation, unseen recoveries are invisible.
- The unused-quota choice and the "alert cursor stays after a resolution" rule are Worker decisions of this change;
  they are documented in the README and are not ratified by any ADR.
- Native Windows author checks had no cgo; the reviewer subsequently ran and passed `-race` on Go 1.23.12/Linux.
