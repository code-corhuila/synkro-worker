# synkro-worker

> Asynchronous jobs and background processing

Part of the **SynkroTech SAS Sales Management System** — organization `code-corhuila`.
Governance and documentation live in [`synkro-docs`](https://github.com/code-corhuila/synkro-docs).

## What the Worker does

Every `LOW_STOCK_EVERY` the Worker runs the **low-stock alert job** (ADR-007 Decision 6, FR-004)
against `synkro-products-api`, using only its public contract and the Worker's SERVICE token
(`products:read`, `stock-alerts:read`, `stock-alerts:write`). It has no database and publishes no
port; Products owns the alerts and the rule that a product has at most one `OPEN` alert.

Each run has two phases:

1. **Open.** Reads products with stock `<= LOW_STOCK_THRESHOLD` (`GET /api/v1/products?stockAtMost=`)
   and opens an alert for each (`POST /api/v1/stock-alerts` with `productId` and `stockAtOpening`).
   Products answers `201` for a new alert and `200` with the existing one when the product already
   has an `OPEN` alert, so repeating the job never duplicates active alerts.
2. **Resolve.** Reads the `OPEN` alerts (`GET /api/v1/stock-alerts?status=OPEN`). For each one whose
   product is above the threshold again (read with `GET /api/v1/products/{id}`, unless the product was
   already read as low in the same run) it calls `POST /api/v1/stock-alerts/{id}/resolve`, which is
   idempotent.

A product that cannot be read (404, timeout, malformed answer) is **never** taken as replenished: its
alert stays as it is and the item is counted as failed.

### Idempotency key and dates

`Idempotency-Key` is `low-stock:<productId>:<date>`, where `<date>` is the **UTC** calendar day
(`YYYY-MM-DD`) at which the Worker decided to open the alert. The contract does not name a time zone;
UTC is this Worker's convention. The key belongs to one intention: every HTTP retry of that intention
sends the same key and payload, even if the day changes while the Worker waits to retry.

### Work budget of a run

Each phase reads at most `LOW_STOCK_MAX_PAGES` pages of `BATCH_SIZE` items (so at most
`BATCH_SIZE * LOW_STOCK_MAX_PAGES` per phase), plus one product read per examined alert that was not
already read as low in that run. The page cursor of each phase is kept in memory between runs and wraps
to page 1 after the last page, so a catalog larger than one run's budget is covered over successive
runs. The `OPEN` alert pages of a run are read **before** any alert is resolved, so resolutions do not
shift the pages still to be read. The open phase may use at most half of the time left before
`RUN_TIMEOUT`, so a slow read cannot leave the resolve phase without budget run after run.
ADR-007 says "up to `BATCH_SIZE` per run"; set `LOW_STOCK_MAX_PAGES=1` to get exactly that.

### Errors, retries and shutdown

| Products answer | Class | Behaviour |
|---|---|---|
| network error, timeout, `429`, `5xx` | transient | up to `HTTP_ATTEMPTS` attempts, exponential backoff with full jitter (base 200 ms, cap 5 s); then the item is failed |
| `401`, `403` | credential | not retried; the **whole run stops** — no further request is made |
| `404` | not found | not retried; the item is failed (never read as "replenished") |
| other `4xx`, any `3xx` | rejected | not retried; the item is failed. Redirects are never followed |
| `2xx` with a missing field, wrong type, wrong page, a product above the filter, an alert of another product, or a body over 1 MiB | malformed | the item is failed; nothing is defaulted (no stock `0` by default) |

- A failed item is counted and the run continues with independent work. If three items in a row end
  unavailable (every attempt spent), the run stops: Products is down.
- Cancellation (shutdown or `RUN_TIMEOUT`) is checked before every item and every page; once observed no
  further request starts, and an in-flight mutation is counted as failed because its outcome is unconfirmed.
- Runs never overlap (the scheduler is sequential and the use case refuses a second concurrent run).
- A panic or an error in a run is logged with the run's correlation id and only ends that run; the
  scheduler waits for the next tick. `RUN_TIMEOUT` and shutdown are **cooperative**: a job that ignores
  its context is not interrupted.
- Each run has its own correlation id, sent as `X-Correlation-Id` on every request and present in every
  log line of the run. Logs carry counts and ids, never the token or response bodies.

## Configuration

See [`.env.example`](.env.example) for every setting, its default and its bounds. `SERVICE_TOKEN` is
required. Any other setting that is set to an invalid value stops the Worker at startup with an error
naming the setting (never its value).

## Known limitations

- **Same-day reopening.** Products replays an `Idempotency-Key` with the alert it first produced, *as it
  is now*. If an alert is opened, resolved, and the stock drops again on the same UTC day, the daily key
  returns the `RESOLVED` alert and no new `OPEN` alert exists until the next UTC day. The Worker
  reports this as a failed item (it does not claim the alert was opened) and does not invent another key
  format. See [`docs/handoff/HU-WRK-02.md`](docs/handoff/HU-WRK-02.md).
- Alerts appear only at the next run, not when stock drops.
- Offset pagination can skip or repeat an item if the catalog changes between page reads; the next pass
  over the list covers it, and repeated opening is idempotent.
- An `OPEN` alert whose product was deactivated cannot be evaluated (Products answers `404` to the
  Worker's token for inactive products): it is counted as failed in every run until someone handles it.
- One global threshold for every product (ADR-007); no per-product threshold.
- This repository does not claim TD-020 as resolved beyond what is described above.

## Branching

Three permanent branches. **None of them accepts a direct commit** — you enter through a child
branch and leave through a Pull Request.

```
develop  <--PR--  feat/... fix/... chore/...
qa       <--PR--  qa-...
main     <--PR--  release/...  hotfix/...
```

Git cannot hold both a branch `qa` and a branch `qa/...`, so the children of `qa` are named
`qa-<description>`. For the same reason, the children of a `release/X.Y.Z` branch are named
`release-<description>`.

Promotion happens **by re-application** (`git cherry-pick -x`), never by merging one permanent
branch into another: `merge develop -> qa` and `merge qa -> main` do not exist in this model.

`main` requires **1 approval from `ariel5253`**. On `develop` and `qa` the team sets its own review
rule.

Full policy: `00-governance/branching-policy.md` in `synkro-docs`.

## Running locally

```bash
SERVICE_TOKEN=<service token issued by synkro-auth-api> \
PRODUCTS_API_URL=http://localhost:8080 \
go run ./cmd/worker
```

Any non-empty `SERVICE_TOKEN` starts the process, but Products rejects a token that Auth did not issue
with the three permissions above, and the run stops at its first request. Stop with Ctrl+C (SIGINT) —
the Worker exits cleanly.

## Running the tests

```bash
go vet ./...
go test -race ./...
```

`-race` needs cgo (a C compiler); on Windows without one, run it in a Linux Go 1.23 container, for example
`docker run --rm -v "$PWD":/src:ro -w /src golang:1.23-bookworm go test -race ./...`.
