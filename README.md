# Viberoxy

A zero-dependency Go daemon that aggregates proxy subscriptions into a pool of reliable xray WANs and exposes them through HTTPS CONNECT and SOCKS5 front-ends.

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
5. **Exposes** all WANs behind a health-aware least-connections load balancer with both an HTTPS CONNECT proxy (`PROXY_PORT`) and a SOCKS5 listener (`SOCKS_PORT`)

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
| `PROXY_PORT` | No | `1080` | User-facing HTTPS CONNECT proxy port |
| `SOCKS_PORT` | No | `0` (off) | User-facing SOCKS5 listener (TCP CONNECT only; UDP ASSOCIATE/BIND rejected with REP 0x07). No auth (RFC 1929 not implemented) |
| `METRICS_PORT` | No | `0` (off) | Prometheus-format `/metrics` + `/healthz` + `/readyz` endpoint port. `/readyz` returns 200 only when at least one **routable** WAN exists (active, under fail threshold, with a live xray process); otherwise returns `503 not ready` |
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
go run .
```

Or with custom WAN count and port:

```bash
SUBSCRIBER_URL="https://example.com/sub" WAN_COUNT=3 PROXY_PORT=8888 go run .
```

The proxy starts as soon as the first WAN passes the `MINIMUM_SPEED` threshold (degraded boot, disable with `ALLOW_DEGRADED_BOOT=false`) and keeps filling slots until `WAN_COUNT` is reached. Debug output is written to `sorted.txt` each cycle.

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
├── parser.go     — subscription parser (ss, vmess, vless, trojan, hysteria2, tuic, wireguard, socks5)
├── xray.go       — xray config builder, process lifecycle
├── tester.go     — SOCKS5 dial, download measurer, speed tester
├── wan.go        — WAN slot state machine (empty→testing→active→draining)
├── proxy.go      — HTTPS CONNECT proxy + load balancer
├── sorted.txt    — per-cycle speed test results (debug output)
```
