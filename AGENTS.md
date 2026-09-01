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
| Change xray config generation | `xray.go` — `BuildXrayConfig`, `buildOutbound` |
| Modify the HTTPS proxy | `proxy.go` — `handleConnect`, load balancing |
| Tune connection latency | `xray.go` (mux), `relay.go` — `tuneTCPConn` (TCP_NODELAY + keepalive) |
| Change split-routing rules | `router.go` — `Router.Decide`, suffix lists |

---

## Directory Map

```
viberoxy/
├── main.go       — env parsing, startup, run loop, cycle logic
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
| `runCycle(cfg, pool, grace)` | `main.go` | One fetch/test/replace cycle |
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
