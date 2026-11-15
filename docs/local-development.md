# Local development

Detailed procedures and delivery notes moved from the repository README at
[source 9e56383](https://github.com/code-corhuila/synkro-worker/blob/9e56383d328866f962abddee8fae45322aff5aa8/README.md).
Recorded execution results retain their original scope; moving them does not
constitute a new execution or certify a different revision.

Run commands from the repository root unless a step explicitly changes directory.

## Running locally

```bash
SERVICE_TOKEN=<service token issued by synkro-auth-api> \
PRODUCTS_API_URL=http://localhost:8080 \
go run ./cmd/worker
```

Any non-empty `SERVICE_TOKEN` starts the process, but Products rejects a token that Auth did not issue
with the three permissions above, and the run stops at its first request. Stop with Ctrl+C (SIGINT) —
the Worker exits cleanly.
