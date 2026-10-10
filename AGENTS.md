# Viberoxy — Agent Guide

> **Purpose:** Let agents find the exact files and patterns they need without reading everything.

---

## Entry Points

| If you need to... | Start here |
|---|---|
| Change env vars or startup flow | `main.go` — `parseConfig`, `startup` |
| Add a new proxy protocol parser | `parser.go` — `ParseSingle` switch |
| Change how speed tests work | `tester.go` — `TestSpeed`, `DownloadMeasurer` |
| Change WAN lifecycle logic | `wan.go` — state machine, drain, health |
| Change WAN control endpoints | `api.go` — WAN/candidate/drop/cycle handlers |
| Change replacement candidate selection | `candidate.go` — candidate pool, ranking, exclusion |
| Change xray config generation | `xray.go` — `BuildXrayConfig`, `buildOutbound` |
| Modify the HTTPS proxy | `proxy.go` — `handleConnect`, load balancing |
| Tune connection latency | `xray.go` (mux), `relay.go` — `tuneTCPConn` (TCP_NODELAY + keepalive) |
| Change split-routing rules | `router.go` — `Router.Decide`, suffix lists |

---

## Directory Map

```
viberoxy/
├── main.go       — env parsing, startup, run loop, cycle logic
├── api.go        — JSON control API for WAN inspection and replacement
├── candidate.go  — tested replacement-candidate pool
├── parser.go     — subscription parser (8 protocols)
├── xray.go       — xray config builder, process lifecycle
├── tester.go     — SOCKS5 dial, download measurer, speed tester
├── wan.go        — WAN slot state machine
├── proxy.go      — HTTPS CONNECT proxy + load balancer
├── sorted.txt    — per-cycle speed test results (debug output)
```

---

## Data Flow (one cycle)

```
Fetch subscription (HTTP GET)
  → ParseConfigs (base64/plain → ProxyConfigs)
  → TestAll (temp xray per config, download & measure Mbps)
  → SortResults (descending by speed)
  → writeSortedTxt
  → Update CandidatePool (top 50 tested configs)
  → Fill empty WAN slots (best configs passing minimum speed)
  → Replace one low-performing WAN (drain old → spawn new on same port)
  → Health-check active xray processes (reap dead/orphaned slots)
```

---

## WAN State Machine

```
empty → testing → active → draining → (kill after max(60, 2×FETCH_INTERVAL)) → empty
```

---

## Key Functions

| Function | File | What it does |
|---|---|---|
| `parseConfig()` | `main.go` | Read & validate env vars, return Config |
| `startup(cfg, ctx)` | `main.go` | Fetch/test until WAN_COUNT active, start proxy, enter loop |
| `runCycle(cfg, pool, candidates, grace)` | `main.go` | One fetch/test/replace cycle and candidate-pool refresh |
| `runLoop(cfg, pool, candidates, proxy, ctx)` | `main.go` | Scheduled/manual cycle loop |
| `ParseConfigs(body)` | `parser.go` | Parse base64/plain subscription text |
| `ParseSingle(raw)` | `parser.go` | Parse one sharelink URI |
| `BuildXrayConfig(cfg, port)` | `xray.go` | Generate xray JSON config for a proxy |
| `StartXray(cfg, port)` | `xray.go` | Write config to temp file, spawn xray process |
| `StopXray(cmd, path)` | `xray.go` | SIGTERM → wait → SIGKILL + cleanup |
| `TestSpeed(cfg, port, timeout, url, size)` | `tester.go` | Speed-test one config through temp xray |
| `TestAll(configs, ...)` | `tester.go` | Test all configs, return sorted results |
| `DownloadMeasurer(addr, url, size, timeout)` | `tester.go` | SOCKS5 dial + HTTP GET + Mbps measurement |
| `NewWANPool(count, basePort)` | `wan.go` | Create N WAN slots |
| `RoutableCount(threshold)` | `wan.go` | Count routable WANs (active, under fail threshold, with running xray) |
| `GetLeastLoaded(thresholds...)` | `wan.go` | Pick least-loaded routable WAN; falls back to degraded if none routable |
| `HealthCheckAll()` | `wan.go` | Check all active/draining slots; reap orphaned (dead xray) processes |
| `DropAndReplace(index, opts)` | `wan.go` | Exclude a WAN, re-test the best candidate, and activate it on the same slot port |
| `NewCandidatePool(maxLen)` | `candidate.go` | Create the ranked, thread-safe replacement pool |
| `NewAPIHandler(pool, cfg)` | `api.go` | Create the WAN/candidate/drop/cycle control API |
| `NewProxyServer(port, pool)` | `proxy.go` | Create HTTPS CONNECT proxy |
| `handleConnect(w, r)` | `proxy.go` | CONNECT handler: pick WAN, SOCKS5 dial, pipe bytes |

---

## Routable WANs

A WAN slot is **routable** when all three conditions hold:

1. Its state is `StateActive` or `StateDraining`.
2. Its `ConsecutiveFails` counter is strictly below the threshold (`DefaultFailThreshold = 2`).
3. Its `Cmd` (xray process handle) is non-nil and the process is alive.

Only routable WANs receive new connections from the load balancer. This prevents blackholing traffic on degraded or dead slots.

### Readiness

`/readyz` returns 200 only when `RoutableCount(DefaultFailThreshold) >= 1`. If every active slot is over the fail threshold or has a dead xray process, readiness returns `503 not ready: no routable WANs`.

### Load-balancer fallback

`GetLeastLoaded` first picks the least-loaded among **routable** slots. If no routable slot exists (all active/draining slots are over the fail threshold), it falls back to the least-loaded among all active/draining slots and logs a warning. This avoids total blackhole when every WAN is degraded but at least one is still alive.

### Orphan reaping

`HealthCheckAll` runs periodically (every `KEEPALIVE_INTERVAL`). It detects active/draining slots whose xray process has died (defunct) and resets them to `StateEmpty` so `StopXray` cleans them up and the slot can be reused.

---

## HTTP Listener Contract

Viberoxy intentionally exposes observability and control on separate listeners:

| Listener | Configuration | Routes |
|---|---|---|
| Observability | `METRICS_PORT` (default `0`, disabled) | `GET /metrics`, `/healthz`, `/readyz` |
| Control API | `API_PORT` (default `1980`) | `GET /api/viberoxy/wans`, `/candidates`, `/cycle`; `POST /cycle/trigger`, `/wans/{index}/drop` |

The control API has mutating routes, no built-in authentication, and currently listens on all interfaces for `API_PORT`. Restrict it with host/container networking or firewall rules to viber-console/trusted operators. Do not mount control routes on the metrics listener merely to simplify deployment.

viber-console must configure both bases independently:

```env
VIBEROXY_METRICS_URL=http://127.0.0.1:2111
VIBEROXY_API_URL=http://127.0.0.1:1980
```

Integration tests must use two distinct HTTP test servers. A single fake server masks wrong-listener bugs that become deployment-only 404 responses.

Both listeners are created inside `startServices`; neither binds during initial candidate testing. With `ALLOW_DEGRADED_BOOT=true`, services start after the first active WAN. Otherwise they start only after the full `WAN_COUNT` is active. A live process with closed service ports during this phase is not by itself proof of a mutex deadlock—check startup logs and candidate failures first.

## Manual WAN Replacement

- `CandidatePool.Update` retains up to 50 results ordered by measured speed.
- `POST /api/viberoxy/wans/{index}/drop` excludes the current raw URI in memory, stops that slot, and re-tests the best eligible candidate before promotion.
- Replacement is serialized with `WANPool.replaceMu`; concurrent drops cannot race process lifecycle operations.
- A successful immediate replacement returns 200. No eligible candidate returns 202 and queues a coalesced cycle. Test/start failures return 502 and leave the slot empty.
- Exclusions are intentionally non-persistent and reset at process restart.

---

## viber-console Supervisor

The supervisor (`viber-console/internal/supervisor`) now auto-restarts crashed child processes:

- Each `Service` has an `AutoRestart` flag (default: `true` for new services).
- When the reaper goroutine observes an unexpected exit and `AutoRestart` is `true`, it waits with exponential backoff (starting at 1s, capped at 30s) then restarts the child.
- `Status()` exposes `restarts`, `auto_restart`, `last_restart_at`, and `last_error` so the API and logs reflect restart activity.
- Restart loops are prevented by the backoff; a fundamentally broken binary will settle into 30s-interval restarts with logged errors.

---

## Conventions

- **Zero dependencies:** Go stdlib only. No external packages.
- **Error handling:** Return errors from functions; log with `slog.Warn`/`Error` at the call site.
- **Logging:** `slog.Info`/`Warn`/`Error` with key-value pairs. Never `log.Println`.
- **Env var config:** All config via env vars. Validated at startup — bad values = hard exit.
- **Thread safety:** `sync.Mutex` per WAN slot for state; `sync/atomic` for connection counters.
