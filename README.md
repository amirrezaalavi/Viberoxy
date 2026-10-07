# Viberoxy

A zero-dependency Go daemon that aggregates proxy subscriptions into a pool of reliable xray WANs and exposes them through HTTPS CONNECT and SOCKS5 front-ends.

[![Go](https://img.shields.io/badge/Go-1.21+-00ADD8?logo=go)](https://go.dev)
[![Zero Dependencies](https://img.shields.io/badge/dependencies-zero-success)](go.mod)

---

## Breaking changes

Recent hardening changed observable behavior — read this before upgrading:

| Change | Before | Now |
|---|---|---|
| **Bind address** | listeners bound all interfaces (`:1080`, `:1980`, …) | everything binds **loopback only** (`127.0.0.1`) unless `LISTEN_ADDR` overrides it; a non-loopback `LISTEN_ADDR` is refused at startup unless `PROXY_USERS` or `API_TOKEN` is set, or `ALLOW_PUBLIC=true` |
| **API credential redaction** | `GET /api/viberoxy/candidates` returned `raw` (the full share link: UUIDs, passwords, keys) | `raw` is gone; entries expose `name`, `server`, `protocol`, `port`, `raw_sha256`, `speed_mbps` (correlate by hash) |
| **Proxy/API authentication** | none possible | optional `PROXY_USERS` (CONNECT 407 Basic + SOCKS5 RFC 1929) and `API_TOKEN` (Bearer for `/api/*` **and** `/metrics`; `/healthz` + `/readyz` stay open for probes) |
| **`XRAY_MUX` default** | `true` | **`false`** (D-04: mux causes head-of-line blocking and is incompatible with `xtls-rprx-vision`); set `XRAY_MUX=true` to opt in |
| **Subscription scheme** | plain `http://` accepted | `https://` only, unless `ALLOW_HTTP_SUBSCRIPTION=true` (startup hard-exits otherwise) |
| **Private targets** | proxied to loopback/LAN/link-local freely | blocked by default (SSRF guard: loopback, link-local incl. `169.254.169.254`, RFC1918, ULA, CGNAT); opt in with `ALLOW_PRIVATE_TARGETS=true` |

---

## How it works

```
Subscription URL → Fetch (https, conditional GET) → Speed test → WAN pool (one xray per slot)
                                                            ↓
                          HTTPS CONNECT / SOCKS5 → health-aware least-loaded selection
                                                            ↓
                                            bidirectional relay → access log + metrics
```

1. **Fetches** a proxy subscription every `FETCH_INTERVAL` seconds (ETag/If-Modified-Since aware; a failed fetch keeps the last good configs).
2. **Speed-tests** each config by downloading a file through a temporary xray instance (ports from a shared allocator, so tests never collide).
3. **Keeps** the top `WAN_COUNT` configs running as persistent xray WANs, one process per slot.
4. **Evaluates continuously**: a full pool still tests up to 2 new candidates per cycle and rotates at most **one** WAN per cycle, make-before-break (old keeps serving until the replacement is tested and started).
5. **Health**: passive per-connection outcome classification feeds a sliding window with ejection + exponential backoff; active canaries (15–30 s) probe every path, with a global environmental breaker.
6. **Exposes** everything behind a health-aware least-connections selector with an HTTPS CONNECT proxy (`PROXY_PORT`), an optional SOCKS5 listener (`SOCKS_PORT`), a control API (`API_PORT`) and a Prometheus/health endpoint (`METRICS_PORT`).

---

## Prerequisites

- **Go 1.21+** (`go.mod` targets `go 1.21.0`)
- **[Xray-core](https://github.com/XTLS/Xray-core)** installed in PATH
- Unix (Linux/macOS). Windows is untested.

---

## Installation

```bash
git clone <your-fork> viberoxy
cd viberoxy
go build -o build/viberoxy .
```

---

## Configuration

All configuration is environment-variable driven; invalid values hard-exit at startup (the error never echoes secrets).

### Core

| Variable | Required | Default | Description |
|---|---|---|---|
| `SUBSCRIBER_URL` | **Yes** | — | Subscription URL (`https://` only unless `ALLOW_HTTP_SUBSCRIPTION=true`) |
| `ALLOW_HTTP_SUBSCRIPTION` | No | `false` | Opt in to a plain `http://` subscription (startup refuses it otherwise) |
| `FETCH_INTERVAL` | No | `300` | Seconds between fetch+test cycles (min 30). Also the rotation cooldown |
| `TEST_TIMEOUT` | No | `10` | Seconds per speed test (min 3) |
| `DOWNLOAD_SIZE` | No | `10000000` | Bytes for the speed-test payload (min 1000000) |
| `DOWNLOAD_ENDPOINT` | No | `https://speed.cloudflare.com/__down?bytes=` | Primary speed-test URL (empty disables it) |
| `DOWNLOAD_FALLBACK` | No | `https://proof.ovh.net/files/` | Fallback speed-test URL |
| `WAN_COUNT` | No | `4` | Max concurrent WANs (1–5) |
| `WAN_BASE_PORT` | No | `10700` | First xray service port (slots use `WAN_BASE_PORT + slot`) |
| `TEST_BASE_PORT` | No | `10800` | First speed-test port (shared allocator range) |
| `MINIMUM_SPEED` | No | `5.0` | Mbps threshold — configs below it are never promoted; incumbents above it are never rotated out |
| `MAX_TEST_PER_CYCLE` | No | `20` | Max configs speed-tested per cycle while the pool has room (1–500). When the pool is **full**, up to 2 NEW candidates are still evaluated per cycle |
| `ACCESS_LOG` | No | `true` | One structured log line per proxied connection |
| `ALLOW_DEGRADED_BOOT` | No | `true` | Start the front-ends as soon as the first WAN is active (vs waiting for a full pool) |
| `DRAIN_MAX` | No | `600` | Seconds: hard stop for a drain. Normally a drain ends earlier, when in-flight flows finish (`inflight == 0`) |

### Listeners, auth and security

| Variable | Default | Description |
|---|---|---|
| `LISTEN_ADDR` | `127.0.0.1` | Bind host for the proxy, SOCKS5, API and metrics listeners. Must be an IP literal. Non-loopback requires `PROXY_USERS`/`API_TOKEN` or `ALLOW_PUBLIC=true` |
| `PROXY_PORT` | `1080` | HTTPS CONNECT proxy port (1–65535) |
| `SOCKS_PORT` | `0` (off) | SOCKS5 listener (TCP CONNECT only; UDP ASSOCIATE/BIND are rejected, REP `0x07`). RFC 1929 user/pass auth is enforced **iff** `PROXY_USERS` is set |
| `METRICS_PORT` | `0` (off) | `/metrics`, `/healthz`, `/readyz` |
| `API_PORT` | `1980` | Control API port (`/api/viberoxy/...`) |
| `PROXY_USERS` | — | Comma-separated `user:password` pairs (split at the **first** colon, so passwords may contain colons). Enables CONNECT 407 Basic and SOCKS5 RFC 1929 |
| `API_TOKEN` | — | Bearer token gating `/api/*` and `/metrics` (probes stay open). No whitespace |
| `ALLOW_PUBLIC` | `false` | Accept a non-loopback `LISTEN_ADDR` without authentication |
| `ALLOW_PRIVATE_TARGETS` | `false` | Allow proxying to loopback/link-local/private destinations (blocked by default) |

### Health

| Variable | Default | Description |
|---|---|---|
| `KEEPALIVE_INTERVAL` | `300` | Base canary interval in seconds, **clamped to the 15–30 s canary band** (values ≥ 30 → 30 s, ≤ 15 → 15 s) and jittered ±20% per tick. It no longer means "probe every 300 s" |
| `WAN_FAIL_THRESHOLD` | `2` | Two meanings: (a) selection excludes a path at this many consecutive failures; (b) canary failures before a slot is marked draining (health flavor, reversible) |
| `HEALTH_ENDPOINTS` | `https://www.gstatic.com/generate_204,https://www.cloudflare.com/cdn-cgi/trace` | Canary endpoints (comma/whitespace separated). A path fails only if **all** endpoints fail; ipify is never used for health (exit-IP reporting only) |
| `STABILITY_PROBES` | `0` (off) | Exit-IP probes per passed speed test (0–5). Ranking only: replacement *prefers* the churniest active WAN, never rejects on this |

### Split routing

| Variable | Default | Description |
|---|---|---|
| `ROUTE_MODE` | `all-proxy` | `all-proxy` (everything via WAN pool), `proxy-default` (WAN unless the host is in the direct list), `direct-default` (direct unless the host is in the proxy list) |
| `DIRECT_DOMAINS` | — | Comma-separated suffixes routed **direct** in `proxy-default` mode (`.ir` matches `ir` and all subdomains) |
| `PROXY_DOMAINS` | — | Comma-separated suffixes routed via **WAN** in `direct-default` mode |
| `DIRECT_LIST_FILE` | — | Newline-separated domain list file, same semantics as `DIRECT_DOMAINS` (`#` comments allowed) |
| `PROXY_LIST_FILE` | — | Newline-separated domain list file, same semantics as `PROXY_DOMAINS` |

### xray

| Variable | Default | Description |
|---|---|---|
| `XRAY_MUX` | `false` | Multiplex client connections over one upstream xray connection per WAN (mux concurrency 8). Amortizes the per-connection TLS/protocol handshake — the biggest lever on setup latency. **Incompatible with `flow=xtls-rprx-vision`** (automatically suppressed there). Keep off for very large single transfers |

> Selection is P2C with session affinity; the only affinity env knob is `NO_AFFINITY_DOMAINS` (comma/suffix list, default empty — hosts listed there are exempt from stickiness). The scheduler's tuning knobs (hysteresis 0.30, `MIN_DWELL` 10m, `MAX_TEST_PER_CYCLE_FULL` 2, spill factor 3x) are code constants for now.

### Quick start

```bash
export SUBSCRIBER_URL="https://example.com/sub"
go run .
```

Or with custom WAN count and port:

```bash
SUBSCRIBER_URL="https://example.com/sub" WAN_COUNT=3 PROXY_PORT=8888 go run .
```

The proxy starts as soon as the first WAN passes `MINIMUM_SPEED` (degraded boot, disable with `ALLOW_DEGRADED_BOOT=false`) and keeps filling slots until `WAN_COUNT` is reached. Debug output is written to `sorted.txt` each cycle.

---

## Architecture

```
            ┌────────────── control plane ──────────────┐
            │ API (API_PORT, Bearer if API_TOKEN set)   │
            │ /metrics /healthz /readyz (METRICS_PORT)  │
            └───────────────┬───────────────────────────┘
                            │
User → HTTPS CONNECT ─┐     │      ┌── cycle (FETCH_INTERVAL): fetch → test → fill → ≤1 rotation swap
User → SOCKS5       ──┤     ▼      │   canaries (15–30s): per-path probes + env breaker
                      ├─ selector (least-loaded, health-aware, retry-aware)
                      │      │
                      │      ▼   path generation = one xray lifetime (Path.ID)
                      │  ┌───────┬───────┬───────┬───────┐
                      │  │ slot 0│ slot 1│ slot 2│ slot 3│  xray on WAN_BASE_PORT+slot
                      │  └───────┴───────┴───────┴───────┘
                      └── direct route (split routing) ──→ target, bypassing the pool
```

### The Path model

Accounting identity is the **path generation** (`internal/path.Path`): one `Path` is minted per xray (re)activation, carries its own inflight counter, lifecycle state and health window, and is never reused for a later occupant. A handler reserves/releases the exact `*Path` it selected, so events from a retired occupant can never land on its replacement.

Path states (`viberoxy_path_state`): `0 probation → 1 active → 2 suspect → 3 draining → 4 dead` (probation is reserved for a future scheduler; nothing assigns it today).

### Slot lifecycle

```
empty → testing → active → draining → empty
```

- **testing:** temp xray on a test port from the shared allocator, run the speed test.
- **active:** xray serving traffic on `WAN_BASE_PORT + slot` (or a spare port during a make-before-break swap).
- **draining:** takes **no new connections** (selection skips it); in-flight flows finish, then the slot is reaped — or at `DRAIN_MAX` (600 s) as the hard stop. A drain ordered by *health* is reversible: when the canary streak recovers, the slot returns to active (a drain ordered by *replacement* is not).

### Selection, failover and readiness

1. **Routable pass:** active slots only, under `WAN_FAIL_THRESHOLD`, non-nil process, not ejected (`suspect`), not already tried on this connection — least-loaded wins, ties go to the lowest slot index.
2. **Degraded fallback:** no routable slot → least-loaded among all *active* slots (even over threshold), so a degraded pool never blackholes.
3. **Last resort:** no active slot at all → a draining slot may take the connection (counted by `viberoxy_selection_last_resort_total` and logged).

Dial-stage retries (`internal/retry`): up to 3 attempts per connection inside a shared 5 s budget, guarded by a token bucket (10 % of admitted traffic, burst 10). Retries happen **only before any application byte flows** — a successful dial is terminal. `503` = no eligible path, `502` = every dial failed (SOCKS5: REP `0x01`).

`/readyz` returns **200 iff at least one ACTIVE routable path exists** (D-03): draining-only or over-threshold pools get `503`. `/healthz` is always 200.

### Health (SPEC-H)

- **Classification at relay end:** `ok` iff bytes reached the client (`down > 0`); a dial error, or data sent with nothing ever delivered, is a hard failure; a client abort (0 up, 0 down) is neutral and ignored.
- **Window:** last 60 s / 30 outcomes. Ejection on ≥ 3 consecutive hard failures across ≥ 3 distinct destination hosts, **or** ≥ 8 outcomes with ≥ 50 % failure ratio. An ejected path becomes `suspect` (not selected).
- **Backoff:** `30 s × 2^ejectCount`, capped at 10 min; decays one step per 10 min of continuous health.
- **Canaries:** every 15–30 s ± 20 % jitter, through each path's SOCKS listener; a path fails only if *all* endpoints fail; half-open re-admission after 2 consecutive successes.
- **Environmental breaker:** ≥ 75 % of paths failing in the same interval ⇒ nothing is ejected/drained, only `viberoxy_env_degraded` moves.

### Rotation (SPEC-R)

At most **one** make-before-break swap per cycle, only when *all* gates pass: `FETCH_INTERVAL` cooldown since the last swap, worst incumbent chosen (churniest exit IP breaks ties), incumbent age ≥ 10 min (`MIN_DWELL`), candidate clears `MINIMUM_SPEED`, no `server:port` already served, no duplicate exit IP, and the candidate is ≥ **30 % better** (hysteresis). Each verdict increments `viberoxy_swap_total{result=swapped|cooldown|dwell|hysteresis|no_candidate}`.

---

## Observability

### Endpoints (`METRICS_PORT`)

| Path | Behavior |
|---|---|
| `/metrics` | Prometheus text exposition. **Requires `Authorization: Bearer <API_TOKEN>` when `API_TOKEN` is set** |
| `/healthz` | Always 200 while the process is up (no auth) |
| `/readyz` | 200 iff ≥ 1 active routable path, else `503 not ready: no routable WANs` (no auth) |

### Metrics

The `path` label is the **path generation ID** (`Path.ID`, monotonic, never reused); `slot` is the stable WAN slot index.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `viberoxy_wans_active` | gauge | — | Active/draining slots |
| `viberoxy_wan_speed_mbps` | gauge | `index` | Last measured speed per slot |
| `viberoxy_wan_stability` | gauge | `index` | Exit-IP churn score (distinct exit IPs − 1) |
| `viberoxy_proxy_connections_total` | counter | `wan`, `proto` | CONNECT attempts handled |
| `viberoxy_proxy_bytes_total` | counter | `wan`, `direction` | Bytes relayed (`wan="direct"` for direct routes) |
| `viberoxy_proxy_latency_seconds` | histogram | — | CONNECT → close |
| `viberoxy_test_duration_seconds` | histogram | — | One speed test |
| `viberoxy_build_info` | gauge | `version` | Build version |
| `viberoxy_path_state` | gauge | `path`, `slot` | 0 probation, 1 active, 2 suspect, 3 draining, 4 dead |
| `viberoxy_path_inflight` | gauge | `path`, `slot` | Connections in flight on that generation |
| `viberoxy_path_ttfb_seconds` | histogram | — | Time-to-first-byte samples |
| `viberoxy_path_goodput_bps` | gauge | `path`, `slot` | Goodput EWMA (bits/s) |
| `viberoxy_conn_outcome_total` | counter | `path`, `outcome`, `reason` | Classified relay outcomes: `ok`/`hard_fail`/`neutral` × `delivered`/`no_down`/`first_byte_timeout`/`client_abort`/`dial_error` |
| `viberoxy_path_ejections_total` | counter | `path`, `reason` | Ejections by rule: `consecutive_distinct` / `fail_ratio` |
| `viberoxy_retry_total` | counter | `stage`, `outcome` | Dial-stage events: `success` / `retry` / `failed` / `no_candidate` |
| `viberoxy_swap_total` | counter | `result` | Rotation verdicts: `swapped`/`cooldown`/`dwell`/`hysteresis`/`no_candidate` |
| `viberoxy_env_degraded` | gauge | — | 1 while the environmental canary breaker is tripped |
| `viberoxy_selection_last_resort_total` | counter | — | Selections that fell back to degraded or (last resort) draining paths |
| `viberoxy_drain_seconds` | histogram | — | How long completed drains lasted |
| `viberoxy_session_reset_on_drain_total` | counter | — | **Must stay 0**: drains never reset a live session |

Per-path gauges are refreshed on every scrape; a retired generation's series are dropped once it holds nothing, so cardinality follows live generations (counters keep their totals).

### Access log

One `slog` line per connection when `ACCESS_LOG=true`:

```
msg="proxy access" target=… wan=… proto=connect|socks5 route=wan|direct bytes_up=… bytes_down=… latency_ms=… status=ok|err
```

`status` is **derived from what happened**, not what was hoped: `ok` only when bytes reached the client (`down > 0`), `err` otherwise — the same rule the health window scores with.

### Control API (`API_PORT`, default 1980)

`GET /api/viberoxy/wans`, `POST /api/viberoxy/wans/{i}/drop`, `GET /api/viberoxy/cycle`, `POST /api/viberoxy/cycle/trigger`, `GET /api/viberoxy/candidates`. All gated by `Authorization: Bearer <API_TOKEN>` when `API_TOKEN` is set. Candidate entries never contain the raw share link (see *Breaking changes*).

---

## Split routing

By default everything goes through the WAN pool (`ROUTE_MODE=all-proxy`). Otherwise each CONNECT/SOCKS5 target is classified before a path is selected:

| Mode | Default | Override list |
|---|---|---|
| `all-proxy` | WAN | none |
| `proxy-default` | WAN | `DIRECT_DOMAINS` / `DIRECT_LIST_FILE` → **direct** |
| `direct-default` | direct | `PROXY_DOMAINS` / `PROXY_LIST_FILE` → **WAN** |

- Suffix matching is case-insensitive; `.example.com` matches `example.com` and all subdomains; a single-label entry (`localhost`) matches exactly. On conflict the explicit list wins (direct list in `proxy-default`, proxy list in `direct-default`).
- Direct connections bypass the pool entirely (no load balancing, no WAN health accounting) and are tuned like proxied ones (`TCP_NODELAY`, keepalive).
- Access log and metrics tag every connection with `route=direct|wan`.

Example — route country-local domains straight, proxy everything else:

```bash
ROUTE_MODE=proxy-default DIRECT_DOMAINS=".ir" go run .
```

### Routing caveats (read this)

- **Matching is on the host string**, not on resolved IPs. A client that resolves DNS locally and connects by **IP literal** bypasses every domain rule: in `proxy-default` an IP-literal target does not match the direct list (it goes via WAN), and in `direct-default` it does not match the proxy list (it goes direct). Anything that must be forced one way needs to be addressed by hostname or handled by the client.
- **Direct-route DNS is resolved locally** (the client's resolver), so split routing does not provide DNS privacy or poisoning protection for direct targets; proxied targets never resolve locally — the WAN's xray does.
- Optional SNI sniffing for IP-literal routing is deferred (D-06).

Example — proxy only geo-blocked domains, go straight for the rest:

```bash
ROUTE_MODE=direct-default PROXY_DOMAINS=".google.com,.youtube.com,.instagram.com" go run .
```

---

## Limitations

- **TCP only: HTTP `CONNECT` and SOCKS5 `CONNECT`.** There is **no UDP ASSOCIATE and no BIND** (rejected: SOCKS5 REP `0x07`, and the xray inbounds are `udp:false`), so **no QUIC / HTTP/3, WebRTC, games, or DNS-over-UDP flows through the proxy**. Use DNS-over-HTTPS/TLS for DNS.
- **No plain-HTTP forward proxy:** non-CONNECT requests to `PROXY_PORT` get `405 Method Not Allowed (CONNECT only)`.
- **Split routing matches the host string** — see the caveats above (IP-literal targets bypass domain rules; direct-route DNS is local).
- **Unix only** (Linux/macOS). Windows is untested (process liveness/stop is not build-tagged per OS).
- **No proxy authentication by default** — set `PROXY_USERS` (the loopback default bind is the baseline protection).
- Session affinity (HRW per `(client, site)`) and the P2C scheduler **are implemented**; the site key is an embedded eTLD+1 heuristic (not a full public-suffix list), client identity is the SOCKS/CONNECT username when `PROXY_USERS` is set (else client IP — NAT caveat), and a sticky pick carrying >3x the median load spills back to P2C.

---

## Project structure

```
viberoxy/
├── main.go              — startup, cycle loop, canary loop, SPEC-R rotation (rotator)
├── wan.go               — WAN pool: slot state machine, selection, drain/un-drain, make-before-break Swap
├── proxy.go             — HTTPS CONNECT front-end
├── socks.go             — SOCKS5 front-end
├── relay.go             — relay, TCP tuning, access log, outcome classification at relay end
├── tester.go            — speed tester (SOCKS5 dial + download measurement + stability probes)
├── xray.go              — thin wrappers over internal/xraycfg + internal/xrayproc
├── health.go            — /metrics, /healthz, /readyz handler + per-path metric refresh
├── metrics.go           — Prometheus-text registry, all viberoxy_* metrics, hook wiring
├── api.go               — control API (WAN slots, candidates, drop, cycle trigger)
├── internal/
│   ├── proxycfg/        — Config, env parsing/validation, subscription parser, split router
│   ├── path/            — Path generation: identity, inflight, state, EWMAs, observer events
│   ├── health/          — SPEC-H: classification, window/ejection/backoff, canaries, breaker
│   ├── retry/           — bounded dial-stage failover (token bucket, T-RETRY safety rules)
│   ├── cands/           — candidate pool (dedupe by Raw, TTL, per-server cap)
│   ├── auth/            — bind policy, PROXY_USERS/API_TOKEN, SSRF guard, redaction helpers
│   ├── subs/            — subscription fetch (size cap, scheme policy, conditional GET)
│   ├── relayio/         — bidirectional splice (idle/handshake timeouts, half-close)
│   ├── xraycfg/         — xray JSON config generation (+ golden tests)
│   ├── xrayproc/        — xray process lifecycle (Wait-based liveness)
│   ├── ports/           — test/spare port allocators
│   └── testutil/        — FakeWAN harness for integration tests
├── scripts/red_gate.sh  — CI gate for the red acceptance suite (empty allow list)
├── README.md
├── AGENTS.md
├── LICENSE
└── go.mod
```

---

## Testing

```bash
go vet ./...
gofmt -l .
go test -race -shuffle=on -count=1 ./...
bash scripts/red_gate.sh          # 11 red tests, empty allow list: any FAIL/SKIP exits 1
```

The red suite (`red_test.go`, build tag `red`) is a set of repros for the original review findings; each fix shrank the allow list, and the list is now empty — every `TestRed_` must pass.
