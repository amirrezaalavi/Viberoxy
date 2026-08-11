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
  → Health-check active xray processes (restart dead ones)
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
| `GetLeastLoaded()` | `wan.go` | Pick WAN with fewest connections |
| `NewProxyServer(port, pool)` | `proxy.go` | Create HTTPS CONNECT proxy |
| `handleConnect(w, r)` | `proxy.go` | CONNECT handler: pick WAN, SOCKS5 dial, pipe bytes |

---

## Conventions

- **Zero dependencies:** Go stdlib only. No external packages.
- **Error handling:** Return errors from functions; log with `slog.Warn`/`Error` at the call site.
- **Logging:** `slog.Info`/`Warn`/`Error` with key-value pairs. Never `log.Println`.
- **Env var config:** All config via env vars. Validated at startup — bad values = hard exit.
- **Thread safety:** `sync.Mutex` per WAN slot for state; `sync/atomic` for connection counters.
