# Testing

Detailed procedures and delivery notes moved from the repository README at
[source 9e56383](https://github.com/code-corhuila/synkro-worker/blob/9e56383d328866f962abddee8fae45322aff5aa8/README.md).
Recorded execution results retain their original scope; moving them does not
constitute a new execution or certify a different revision.

Run commands from the repository root unless a step explicitly changes directory.

## Running the tests

```bash
go vet ./...
go test -race ./...
```

`-race` needs cgo (a C compiler); on Windows without one, run it in a Linux Go 1.23 container, for example
`docker run --rm -v "$PWD":/src:ro -w /src golang:1.23-bookworm go test -race ./...`.

### Opt-in acceptance against a real Products

`internal/acceptance` drives the real use case and the real HTTP client against a running Products API and its
PostgreSQL. It is excluded from `go test ./...` by the `acceptance` build tag, and each test skips itself unless
its fixtures are provided through the environment (`TEST_PRODUCTS_URL`, `TEST_SERVICE_TOKEN`, and the fixture ids
named in the test). The token is read from the environment and never printed. Fixtures are isolated products
prepared through the Products API; the tests create and delete nothing and use no SQL. See
[`docs/handoff/HU-WRK-02-run-contract.md`](handoff/HU-WRK-02-run-contract.md) for the fixtures and the command.
