# Upstream sync

This tree incorporates the upstream `FlareSolverr/FlareSolverr` master snapshot at
`0f05ed8fc974b215c36892b5a30122e27fe3c671`.

The fork-specific websocket behavior is intentionally retained:

- `GET /websocket_messages` supports global and session-scoped queues and clears the queue after reads.
- `GET /websocket_logger/messages` exposes the persistent logger queue.
- Frame capture retains sent/received type, payload, URL, ordering, and bounded retention semantics.
- `TARGET_URL`, `WEBSOCKET_MAX_MESSAGES`, and `SESSION_RELOAD_INTERVAL` remain configurable.

The environment does not permit writes to `.git`, so the upstream delta was reconciled in the working tree rather than recorded as a merge commit. The merge base used for comparison was `e3c300ee0f2a44e8461660d0f777df956ea1b86d`.
