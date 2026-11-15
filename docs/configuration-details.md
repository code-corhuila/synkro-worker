# Configuration details

Detailed procedures and delivery notes moved from the repository README at
[source 9e56383](https://github.com/code-corhuila/synkro-worker/blob/9e56383d328866f962abddee8fae45322aff5aa8/README.md).
Recorded execution results retain their original scope; moving them does not
constitute a new execution or certify a different revision.

Run commands from the repository root unless a step explicitly changes directory.

## Configuration

See [`.env.example`](../.env.example) for every setting, its default and its bounds. `SERVICE_TOKEN` is
required. Any other setting that is set to an invalid value stops the Worker at startup with an error
naming the setting (never its value).
