# Viberoxy — Proxy Aggregation Daemon

A zero-dependency Go daemon that fetches proxy subscriptions, speed-tests configs,
maintains a pool of xray WANs, and exposes a single HTTPS proxy entry point.

## Principles

- Zero external dependencies (Go stdlib only)
- Daemon runs forever; graceful shutdown on SIGINT/SIGTERM
- All env vars validated at startup — bad values = hard exit with clear message
- Required vars must be set; optional vars have safe defaults
- Reliability over speed — conservative rotation, no mid-session breaks

## Env Vars

| Var | Required | Validation | Default |
|---|---|---|---|
| `SUBSCRIBER_URL` | **Yes** | Valid URL (http/https) | — |
| `FETCH_INTERVAL` | No | ≥ 30 seconds | `300` |
| `TEST_TIMEOUT` | No | ≥ 3 seconds | `10` |
| `DOWNLOAD_SIZE` | No | ≥ 1_000_000 bytes | `10000000` |
| `DOWNLOAD_ENDPOINT` | No | Valid URL | `https://speed.cloudflare.com/__down?bytes=` |
| `DOWNLOAD_FALLBACK` | No | Valid URL | `https://proof.ovh.net/files/` |
| `WAN_COUNT` | No | 1–5, integer | `4` |
| `WAN_BASE_PORT` | No | 1–65535, integer | `10700` |
| `TEST_BASE_PORT` | No | 1–65535, integer | `10800` |
| `PROXY_PORT` | No | 1–65535, integer | `1080` |
| `MINIMUM_SPEED` | No | ≥ 0.1 Mbps, float | `5` |

On any invalid value: log the field, value, expected format, then `os.Exit(1)`.

## Startup Sequence

1. Parse & validate all env vars
2. Fetch subscription, parse configs
3. Speed-test configs sequentially until `WAN_COUNT` configs pass `MINIMUM_SPEED`
4. Start xray for each qualifying config on `WAN_BASE_PORT + index`
5. Start HTTPS proxy on `PROXY_PORT`
6. Enter main loop

If startup fails to find `WAN_COUNT` qualifying configs, retries indefinitely
(does not start proxy until enough WANs are active).

## Main Loop (every FETCH_INTERVAL seconds)

1. Fetch + parse subscription
2. Speed-test **all** configs (temp xray on `TEST_BASE_PORT + index`, one at a time)
3. Sort by speed → write `sorted.txt`
4. Among active WANs, find one below `MINIMUM_SPEED` (or the slowest)
5. If candidate config exists that's faster and not active, begin replacement:
   - Mark slot as `draining`
   - After `max(60, 2 × FETCH_INTERVAL)` seconds → kill old xray → start new xray
     with candidate config on same service port → mark `active`
6. Check health of all active xray processes; restart any that died

## WAN State Machine

```
empty → testing → active → draining → (kill after grace) → empty
```

- **testing:** temp xray on `TEST_BASE_PORT + index`, speed test
- **active:** xray on `WAN_BASE_PORT + index`, serving traffic via proxy
- **draining:** xray still running, load balancer skips this slot for new connections.
  After `max(60, 2 × FETCH_INTERVAL)` seconds → kill → slot becomes empty

## Port Architecture

| Range | Purpose |
|---|---|
| `WAN_BASE_PORT + 0..N` | Service xray instances (persistent per slot) |
| `TEST_BASE_PORT + 0..N` | Temp xray for speed tests (start → test → kill per cycle) |

Service port per slot never changes. Load balancer routes to `WAN_BASE_PORT + slot_index`.

## HTTPS Proxy

- Go `net/http` server with CONNECT handler
- On accept, pick active WAN with fewest active connections
- Dial xray local SOCKS inbound on that WAN's port
- Bidirectional `io.Copy` until connection ends
- Per-WAN connection counter (atomic)

## File Structure

```
viberoxy/
├── main.go       — env parsing, signals, main loop orchestration
├── proxy.go      — HTTPS CONNECT server, load balancer
├── xray.go       — xray process lifecycle (start, health, kill)
├── wan.go        — WAN slot state machine, connection counter
├── tester.go     — speed test: download + measure + sort
├── parser.go     — subscription parser (from Viberayd, stdlib only)
└── sorted.txt    — per-cycle debug output
```

## Risks & Mitigations

| Risk | Mitigation |
|---|---|
| Big sub → slow first startup | Proxy starts only when `WAN_COUNT` configs pass `MINIMUM_SPEED` |
| Test port conflicts with service port | Distinct ranges (10700 vs 10800); per-slot lock during replacement |
| Xray binary missing | Clear error on startup |
| All configs below minimum speed | WANs stay empty, proxy never starts; logs loudly each cycle |
| Speed test endpoint down | Fallback URL; if both fail, skip test cycle, keep current WANs |
| Test download hangs | `TEST_TIMEOUT` context per config |
| Config churn on adequate WANs | `MINIMUM_SPEED` gate — WANs above threshold never replaced |
