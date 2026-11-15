# Known limitations

Detailed procedures and delivery notes moved from the repository README at
[source 9e56383](https://github.com/code-corhuila/synkro-worker/blob/9e56383d328866f962abddee8fae45322aff5aa8/README.md).
Recorded execution results retain their original scope; moving them does not
constitute a new execution or certify a different revision.

Run commands from the repository root unless a step explicitly changes directory.

## Known limitations

- **Observed, not event-driven.** If stock drops and recovers entirely between two observations, the Worker
  cannot see that episode and cannot promise a separate alert for it (ADR-018).
- **No fencing.** A client timeout does not prove that Products stopped a request, and stock observation and
  alert mutation are not atomic. The design gives periodic reconciliation, request-key deduplication and
  Products' one-`OPEN`-alert invariant, not exactly-once episode detection.
- **Mapping growth.** One key mapping per visited low-stock product per run (see above); no cleanup or
  retention window is established.
- **Unused quota.** A phase with nothing to do does not lend its share to the other one.
- Detection waits for a successful eligible pass to reach the product; pagination, quotas and time limits can defer it beyond the next tick.
- Offset pagination can skip or repeat an item if the catalog changes between page reads; the next pass
  over the list covers it, and repeated opening is idempotent.
- An `OPEN` alert whose product was deactivated cannot be evaluated (Products answers `404` to the
  Worker's token for inactive products): it is counted as failed in every run until someone handles it.
- One global threshold for every product (ADR-007); no per-product threshold.
- This repository does not claim TD-020 as resolved beyond what is described above.
