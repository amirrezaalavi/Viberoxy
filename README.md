# Viberoxy

A zero-dependency Go daemon that aggregates proxy subscriptions into a pool of reliable xray WANs and exposes them through HTTPS CONNECT and SOCKS5 front-ends.

[![Go](https://img.shields.io/badge/Go-1.21+-00ADD8?logo=go)](https://go.dev)
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
5. **Exposes** all WANs behind a health-aware least-connections load balancer with both an HTTPS CONNECT proxy (`PROXY_PORT`) and a SOCKS5 listener (`SOCKS_PORT`)
6. **Maintains** a pool of tested replacement candidates and exposes a private control API for inspecting, dropping, and replacing WANs

---

## Prerequisites

- **Go 1.21+**
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
| `PROXY_PORT` | No | `1080` | User-facing HTTPS CONNECT proxy port |
| `SOCKS_PORT` | No | `0` (off) | User-facing SOCKS5 listener (TCP CONNECT only; UDP ASSOCIATE/BIND rejected with REP 0x07). No auth (RFC 1929 not implemented) |
| `METRICS_PORT` | No | `0` (off) | Prometheus-format `/metrics` + `/healthz` + `/readyz` endpoint port. `/readyz` returns 200 only when at least one **routable** WAN exists (active, under fail threshold, with a live xray process); otherwise returns `503 not ready` |
| `API_PORT` | No | `1980` | Private JSON control API for WAN state, candidates, manual replacement, and cycle control. Keep this listener on a trusted interface; it includes mutating endpoints and has no built-in authentication |
| `MINIMUM_SPEED` | No | `5.0` | Mbps threshold — don't replace WANs above this |
| `MAX_TEST_PER_CYCLE` | No | `20` | Max configs speed-tested per runCycle (subscription is latency-sorted, so testing beyond this is wasted) |
| `KEEPALIVE_INTERVAL` | No | `300` | Seconds between end-to-end WAN health probes (min 10) |
| `WAN_FAIL_THRESHOLD` | No | `2` | Consecutive probe/dial failures before a WAN is excluded from load balancing and marked draining |
| `STABILITY_PROBES` | No | `0` (off) | Exit-IP probes per passed speed test (0-5). When >0, WANs are ranked by exit-IP stability (lower = more stable) and replacement prefers the least-stable active WAN. Ranking only — churny configs are never rejected |
| `ACCESS_LOG` | No | `true` | One structured log line per proxied connection |
| `ALLOW_DEGRADED_BOOT` | No | `true` | Start the proxy as soon as the first WAN is active (vs waiting for full WAN_COUNT) |
| `ROUTE_MODE` | No | `all-proxy` | Split-routing policy: `all-proxy` (everything via WAN pool — historical behavior), `proxy-default` (everything via WAN except direct-list hosts), `direct-default` (everything direct except proxy-list hosts) |
| `DIRECT_DOMAINS` | No | — | Comma-separated domain suffixes routed direct in `proxy-default` mode (e.g. `.ir` for country-local domains). `.example.com` matches `example.com` and subdomains |
| `PROXY_DOMAINS` | No | — | Comma-separated domain suffixes routed via WAN in `direct-default` mode |
| `DIRECT_LIST_FILE` | No | — | Path to a newline-separated domain list file (same semantics as `DIRECT_DOMAINS`, `#` comments allowed) |
| `PROXY_LIST_FILE` | No | — | Path to a newline-separated domain list file (same semantics as `PROXY_DOMAINS`) |
| `XRAY_MUX` | No | `true` | Multiplex client connections over one upstream xray connection per WAN (mux concurrency 8). Amortizes the TLS/protocol handshake that otherwise runs per connection — the biggest lever on per-connection setup latency. Disable for workloads dominated by very large single transfers |

### Quick start

```bash
export SUBSCRIBER_URL="https://example.com/sub"
export METRICS_PORT=2111
export API_PORT=1980
go run .
```

Or with custom WAN count and port:

```bash
SUBSCRIBER_URL="https://example.com/sub" WAN_COUNT=3 PROXY_PORT=8888 go run .
```

The proxy starts as soon as the first WAN passes the `MINIMUM_SPEED` threshold (degraded boot, disable with `ALLOW_DEGRADED_BOOT=false`) and keeps filling slots until `WAN_COUNT` is reached. Debug output is written to `sorted.txt` each cycle.

When running with [viber-console](https://github.com/yolka-wiz/viber-console), point its two Viberoxy clients at the matching listeners:

```bash
VIBEROXY_METRICS_URL=http://127.0.0.1:2111
VIBEROXY_API_URL=http://127.0.0.1:1980
```

Observability and control deliberately use separate listeners. This keeps the mutating WAN controls away from networks that only need to scrape metrics.

The proxy, observability listener, and control API are started only after Viberoxy promotes a WAN. With the default `ALLOW_DEGRADED_BOOT=true`, that means after the first WAN becomes active; with degraded boot disabled, Viberoxy waits for all `WAN_COUNT` slots. During initial subscription testing, ports may therefore remain closed even though the process is alive.

### Supported xray outbounds

Viberoxy promotes Shadowsocks, VMess, VLESS, Trojan, and SOCKS5 configs. VLESS and Trojan Reality links preserve `pbk` (public key), `sid` (short ID), and `spx` (SpiderX), and `xhttp` transport is supported alongside TCP/HTTP, WebSocket, and gRPC. Hysteria2, TUIC, and WireGuard links may be parsed from a subscription but are skipped during WAN promotion because they are not implemented as xray outbounds here.

---

## Architecture

```
User → HTTPS CONNECT (PROXY_PORT) ─┐
User → SOCKS5          (SOCKS_PORT) ─┤
                                     ↓
                        Load Balancer (least-connections, health-aware)
                                     ↓
    ┌──────┬──────┬──────┬──────┐
    │WAN 0 │WAN 1 │WAN 2 │WAN 3 │  (xray: 10700-10703)
    └──────┴──────┴──────┴──────┘
                ↑
          Speed Tester (10800+) + stability probes (optional)
                ↑
          Fetcher ← SUBSCRIBER_URL (latency-sorted)

Operator → Observability (METRICS_PORT) → /metrics, /healthz, /readyz
Operator → Control API  (API_PORT)     → WAN state, candidates, drop, cycle
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

### Candidate Pool and Manual WAN Replacement

Successful cycle results are retained in a thread-safe candidate pool, ordered by measured speed. The pool is capped at 50 entries. A manually dropped WAN is excluded in memory, stopped, and replaced on the same fixed slot port with the best eligible candidate after that candidate passes a fresh test.

If no eligible candidate is available, the slot remains empty and Viberoxy queues a new fetch/test cycle. Exclusions are intentionally in-memory only and reset when Viberoxy restarts.

---

## HTTP Endpoints

Viberoxy uses two independent HTTP listeners:

### Observability (`METRICS_PORT`)

Disabled by default. Set `METRICS_PORT` to a non-zero port to enable it.

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/metrics` | Prometheus metrics |
| `GET` | `/healthz` | Process liveness |
| `GET` | `/readyz` | Readiness; 200 only when at least one WAN is routable |

### Control API (`API_PORT`, default `1980`)

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/viberoxy/wans` | Per-slot state, speed, connections, exit IP, last probe, and failure count |
| `GET` | `/api/viberoxy/candidates` | Current tested replacement candidates |
| `GET` | `/api/viberoxy/cycle` | Last cycle, next cycle, and countdown |
| `POST` | `/api/viberoxy/cycle/trigger` | Queue an immediate fetch/test cycle; repeated requests are coalesced |
| `POST` | `/api/viberoxy/wans/{index}/drop` | Exclude and replace one WAN; returns 200 when replaced or 202 when a new cycle was queued |

The control API has no built-in authentication and currently listens on all interfaces for its configured port. Restrict it with host/container networking or firewall rules so only viber-console and trusted operators can reach it; do not publish port `1980` directly to the internet.

---

## Split Routing

By default everything goes through the WAN pool (`ROUTE_MODE=all-proxy`, the historical behavior). When a router is configured, each CONNECT/SOCKS5 target is classified before the WAN is selected:

| Mode | Default | Override list |
|---|---|---|
| `all-proxy` | WAN | none |
| `proxy-default` | WAN | `DIRECT_DOMAINS` / `DIRECT_LIST_FILE` → **direct** |
| `direct-default` | direct | `PROXY_DOMAINS` / `PROXY_LIST_FILE` → **WAN** |

- Suffix matching is case-insensitive; `.example.com` matches `example.com` and all subdomains; a single-label entry (`localhost`) matches exactly.
- Direct connections bypass the WAN pool entirely (no load balancing, no WAN health accounting) and are tuned like proxied ones (`TCP_NODELAY`, keepalive).
- Access log and metrics tag every connection with `route=direct|wan` so split behavior is observable (`viberoxy_proxy_bytes_total{route="direct"}` etc.).

Example — route Iranian country-local domains straight, proxy everything else:

```bash
ROUTE_MODE=proxy-default DIRECT_DOMAINS=".ir" go run .
```

Example — proxy only geo-blocked domains, go straight for the rest:

```bash
ROUTE_MODE=direct-default PROXY_DOMAINS=".google.com,.youtube.com,.instagram.com" go run .
```

Note: DNS resolution stays with the client in v1 (TCP routing only). If a proxied domain's DNS is poisoned locally, the client should use DNS-over-HTTPS for it.

---

## Project Structure

```
viberoxy/
├── main.go       — env parsing, startup, run loop, cycle logic
├── api.go        — private JSON control API (WANs, candidates, drop, cycle)
├── candidate.go  — thread-safe tested replacement-candidate pool
├── parser.go     — subscription parser (ss, vmess, vless, trojan, hysteria2, tuic, wireguard, socks5)
├── xray.go       — xray config builder, process lifecycle
├── tester.go     — SOCKS5 dial, download measurer, speed tester
├── wan.go        — WAN slot state machine (empty→testing→active→draining)
├── proxy.go      — HTTPS CONNECT proxy + load balancer
├── sorted.txt    — per-cycle speed test results (debug output)
```
