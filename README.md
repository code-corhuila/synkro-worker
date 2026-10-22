# synkro-worker

> Asynchronous jobs and background processing

Part of the **SynkroTech SAS Sales Management System** — organization `code-corhuila`.
Governance and documentation live in [`synkro-docs`](https://github.com/code-corhuila/synkro-docs).

## Delivery scope

> Esta entrega contiene la estructura y el planificador. El job de stock bajo todavía es provisional y no realiza operaciones contra Products.

In English: this delivery contains the project structure and the scheduler only. The job wired in
`cmd/worker/main.go` is a placeholder that logs a message and returns; it does not query Products,
open or resolve alerts, or make any HTTP call. `SERVICE_TOKEN` is required at startup, but nothing
uses it yet, so no authentication against Products has been exercised.

`RUN_TIMEOUT` sets a deadline on the context passed to each job run. The deadline is cooperative: a
job that ignores its context is not interrupted, and it delays the next run and the shutdown.

Known limitations of this delivery:

- Only `SERVICE_TOKEN` is validated. Malformed or non-positive durations fall back to their defaults
  without an error, and integer settings accept any value, including negative ones.
- A panic inside a job is not recovered and ends the process.
- `internal/domain`, `internal/application` and `internal/adapter/out/products` hold only
  placeholders, so the architecture check in CI has no domain code to inspect yet.
- The job is a placeholder, as described above.

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
SERVICE_TOKEN=dev-placeholder go run ./cmd/worker
```

Logs one line per job run. Stop with Ctrl+C (SIGINT) — the worker exits
cleanly.

## Running the tests

```bash
go test -race ./...
```
