# Viberoxy

A zero-dependency Go daemon that aggregates proxy subscriptions into a pool of reliable xray WANs and exposes them through a single HTTPS proxy.

[![Go](https://img.shields.io/badge/Go-1.26+-00ADD8?logo=go)](https://go.dev)
[![Zero Dependencies](https://img.shields.io/badge/dependencies-zero-success)](go.mod)

---

## How It Works

```
Subscription URL → Fetch → Speed test → Sort → WAN pool (xray) → Load balancer → HTTPS Proxy
```

1. **Fetches** a proxy subscription URL every N seconds
2. **Speed-tests** each config by downloading a file through a temporary xray instance
3. **Keeps** the top N configs running as persistent xray WANs on fixed ports
4. **Rotates** slow configs one at a time (graceful drain, no mid-session breaks)
5. **Exposes** all WANs behind a single HTTPS CONNECT proxy with least-connections load balancing

---

## Prerequisites

- **Go 1.26+**
- **[Xray-core](https://github.com/XTLS/Xray-core)** installed in PATH

---

## Installation

```bash
git clone <your-fork> viberoxy
cd viberoxy
go build -o build/viberoxy .
```

---

## Configuration

All config via environment variables:

| Variable | Required | Default | Description |
|---|---|---|---|
| `SUBSCRIBER_URL` | **Yes** | — | Subscription URL to fetch |
| `FETCH_INTERVAL` | No | `300` | Seconds between fetch+test cycles (min 30) |
| `TEST_TIMEOUT` | No | `10` | Seconds per speed test (min 3) |
| `DOWNLOAD_SIZE` | No | `10000000` | Bytes for speed test payload (min 1000000) |
| `DOWNLOAD_ENDPOINT` | No | `https://speed.cloudflare.com/__down?bytes=` | Primary speed test URL |
| `DOWNLOAD_FALLBACK` | No | `https://proof.ovh.net/files/` | Fallback speed test URL |
| `WAN_COUNT` | No | `4` | Max concurrent WANs (1–5) |
| `WAN_BASE_PORT` | No | `10700` | First xray service port |
| `TEST_BASE_PORT` | No | `10800` | First xray test port |
| `PROXY_PORT` | No | `1080` | User-facing HTTPS proxy port |
| `MINIMUM_SPEED` | No | `5.0` | Mbps threshold — don't replace WANs above this |

### Quick start

```bash
export SUBSCRIBER_URL="https://example.com/sub"
go run .
```

Or with custom WAN count and port:

```bash
SUBSCRIBER_URL="https://example.com/sub" WAN_COUNT=3 PROXY_PORT=8888 go run .
```

The proxy will start only when `WAN_COUNT` configs pass the `MINIMUM_SPEED` threshold. Debug output is written to `sorted.txt` each cycle.

---

## Architecture

```
User → HTTPS CONNECT (PROXY_PORT)
                ↓
         Load Balancer (least-connections)
                ↓
    ┌──────┬──────┬──────┬──────┐
    │WAN 0 │WAN 1 │WAN 2 │WAN 3 │  (xray: 10700-10703)
    └──────┴──────┴──────┴──────┘
                ↑
          Speed Tester (10800+)
                ↑
          Fetcher ← SUBSCRIBER_URL
```

### WAN Lifecycle

Each slot transitions through: `empty → testing → active → draining → empty`

- **Testing:** temp xray on `TEST_BASE_PORT + slot`, run speed test
- **Active:** xray on `WAN_BASE_PORT + slot`, serving traffic
- **Draining:** xray still running but no new connections routed to it. Killed after `max(60s, 2×FETCH_INTERVAL)`

### Rotation Policy

- Only **one WAN replaced per cycle** (prevents mass disconnects)
- WANs running above `MINIMUM_SPEED` are never replaced
- Service ports are fixed per slot — the load balancer never needs to update its routing

---

## Project Structure

```
viberoxy/
├── main.go       — env parsing, startup, run loop, cycle logic
├── parser.go     — subscription parser (ss, vmess, vless, trojan, hysteria2, tuic, wireguard, socks5)
├── xray.go       — xray config builder, process lifecycle
├── tester.go     — SOCKS5 dial, download measurer, speed tester
├── wan.go        — WAN slot state machine (empty→testing→active→draining)
├── proxy.go      — HTTPS CONNECT proxy + load balancer
├── sorted.txt    — per-cycle speed test results (debug output)
```
