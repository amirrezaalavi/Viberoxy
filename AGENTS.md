# Viberoxy — Agent Guide

> **Purpose:** Let agents find the exact files and patterns they need without reading everything.

---

## Entry Points

| If you need to... | Start here |
|---|---|
| Change env vars / startup validation | `internal/proxycfg/config.go` — `ParseConfig()` (all knobs, hard-exit on bad values); `main.go:main()` only calls it |
| Add a new proxy protocol parser | `internal/proxycfg/parse.go` — `ParseSingleErr` switch (+ `IsXraySupported` leak guard; hysteria2/tuic/wireguard must never be promoted) |
| Change split-routing rules | `internal/proxycfg/router.go` — `Router.Decide`, suffix lists (host-string match) |
| Change how speed tests work | `tester.go` — `TestSpeedWithStability`, `DownloadMeasurer`; mux parity via `xray.go:xrayMuxForRun` |
| Change xray config generation | `internal/xraycfg/build.go` — `BuildXrayConfig`, `buildOutbound` (`xray.go` is a thin wrapper + type alias) |
| Health-check / stop a running xray | `internal/xrayproc/proc.go` — `Handle.Alive/Stop/Exited` (Wait-based liveness); wrappers `HealthCheckXray`/`StopXray` in `xray.go` |
| Change WAN lifecycle / selection / drain | `wan.go` — slot state machine, `Select`, `DrainExpired`, `UnDrainIfRecovered`, `DropAndReplace`/`Swap` |
| Change path identity / inflight accounting | `internal/path/path.go` — one `Path` per xray lifetime (F-04 root fix) |
| Change passive health / canaries | `internal/health/` — `Classify`/`ClassifyReason`, `State` window/eject/backoff, `ApplyCanary`; wiring in `main.go:runCanaries` |
| Change selection ranking (P2C / cost model) | `internal/sched/sched.go` — `Select`, cost/weight/ramp, atomic select+reserve; tier partitioning in `wan.go:Select` |
| Change session affinity | `internal/affinity/affinity.go` — HRW `Pick`, `SiteKey`, `Sticky` (`NO_AFFINITY_DOMAINS`); keys built in `proxy.go`/`socks.go` |
| Change dial-stage failover | `internal/retry/retry.go` — `Run`, `Policy`, token bucket; wiring in `relay.go:dialWANFailover` |
| Change the candidate pool | `internal/cands/pool.go` — dedupe by Raw, TTL, `Best(exclude)` |
| Change auth / bind policy / SSRF guard | `internal/auth/auth.go` — `EnsureBindAllowed`, `CheckUsers`, `CheckBearer`, `TargetAllowed`; front-end wiring in `proxy.go`/`socks.go`/`api.go`, `main.go:gateMetrics` |
| Change subscription fetching | `internal/subs/fetch.go` — size cap, scheme policy, conditional GET; `main.go:fetchSubscription` logs/replays |
| Modify the HTTPS proxy / SOCKS5 front-end | `proxy.go` — `handleConnect`; `socks.go` — handshake + auth |
| Tune connection latency | `XRAY_MUX` env (`internal/xraycfg`, mux suppressed with `flow=`); `relay.go:tuneTCPConn` (TCP_NODELAY + keepalive) |
| Change metrics / readiness | `metrics.go` (registry + all `viberoxy_*` metrics + hook wiring), `health.go` (`/metrics`, `/healthz`, `/readyz`, per-path refresh) |
| Change the access log | `relay.go:logAccess` — status derived from reality: `ok` iff `down > 0` |
| Change rotation | `main.go` — `rotator.decide` / `maybeSwap` (SPEC-R gates, ≤1 swap per cycle) |

## Directory Map

```
viberoxy/
├── main.go              — startup, fetch/subscription glue, runLoop/runCycle, canary loop, SPEC-R rotator
├── wan.go               — WAN pool: slot state machine, Select (3-pass eligibility + sched/affinity), drain/un-drain, Swap/DropAndReplace
├── proxy.go             — HTTPS CONNECT front-end (405 for non-CONNECT)
├── socks.go             — SOCKS5 front-end (TCP CONNECT only; RFC 1929 auth)
├── relay.go             — relay, TCP tuning, access log, outcome classification at relay end
├── tester.go            — speed tester (SOCKS5 dial + download measurement + stability probes)
├── xray.go              — thin wrappers over internal/xraycfg + internal/xrayproc (+ xrayMuxForRun)
├── health.go            — /metrics, /healthz, /readyz handler + refreshPoolMetrics (path series, env gauge)
├── metrics.go           — hand-rolled Prometheus-text registry, all viberoxy_* metrics, hook wiring
├── api.go               — control API (WAN slots, candidates redacted, drop-and-replace, cycle trigger)
├── internal/
│   ├── proxycfg/        — Config + ParseConfig (env), subscription parser, split router
│   ├── path/            — Path generation: identity, inflight, state, EWMAs, observer events
│   ├── health/          — SPEC-H: Classify/ClassifyReason, window/eject/backoff, canaries, env breaker
│   ├── sched/           — selection ranking: P2C, peak-EWMA cost, slow-start, atomic select+reserve
│   ├── affinity/        — HRW (client, site) stickiness, eTLD+1 site keys, NO_AFFINITY_DOMAINS
│   ├── retry/           — bounded dial-stage failover (T-RETRY safety: dial stage only)
│   ├── cands/           — candidate pool (dedupe by Raw, TTL, per-server cap)
│   ├── auth/            — bind policy, PROXY_USERS/API_TOKEN, SSRF guard, redaction
│   ├── subs/            — subscription fetch (8 MiB cap, https policy, ETag/Last-Modified)
│   ├── relayio/         — bidirectional splice (idle/handshake timeouts, half-close, peek reader)
│   ├── xraycfg/         — xray JSON config generation + golden tests
│   ├── xrayproc/        — xray process lifecycle (Wait-based liveness, zombie reaping)
│   ├── ports/           — speed-test + spare-service port allocators (F-13)
│   └── testutil/        — FakeWAN harness for integration tests
├── integrity_test.go    — T-INT-01 cross-talk-under-chaos + T-INT-02 transfer-across-swap (see Verification suites)
├── chaos_test.go        — T-CHAOS-01..09 budget suite (build tag `chaos`)
├── scripts/red_gate.sh  — CI gate for the red acceptance suite (allow list now EMPTY)
├── README.md            — user docs: env table, breaking changes, limitations (source of truth for knobs)
├── AGENTS.md            — this file (writes require user approval; drafts live in agents_temp.md)
├── LICENSE
└── go.mod               — module viberoxy, go 1.21.0, zero dependencies
```

Deleted/renamed files older guides still referenced: `parser.go`
(→ `internal/proxycfg/parse.go`), root `router.go` (→ `internal/proxycfg/router.go`),
`candidate.go` (→ `internal/cands/pool.go`), root `health2.go` never existed.
There is no `viber-console` supervisor in this repo (F-21).

## Data Flow

**Startup** (`startup` in `main.go`): `proxycfg.ParseConfig` → build the pool,
port allocators (F-13) → fetch subscription → speed-test configs one by one
(`TestSpeedWithStability`, temp xray per config on a port from the shared
allocator) → promote passers (≥ `MINIMUM_SPEED`) into slots
(`StartTesting` → `StartXray` → `SetActive`, which mints a fresh `*path.Path`)
until the pool is full. With degraded boot the front-ends start after the first
active WAN. Then `runLoop`.

**Every cycle** (`runCycle`, ticker `FETCH_INTERVAL` or API trigger):

```
reapCompletedDrains(drainMax)     — inflight==0 or DRAIN_MAX; records viberoxy_drain_seconds;
                                    health-drained slots may un-drain inside DrainExpired instead
HealthCheckAll()                  — reap slots whose xray process died (internal/xrayproc)
fetchSubscription()               — internal/subs; 304 replays cached configs; failure keeps the pool
test loop                          — fill empty slots; when FULL, still evaluate ≤ 2 NEW candidates
                                     (maxTestPerCycleFull) so rotation is never starved
writeSortedTxt + cands.Update      — debug output + candidate pool
rotation.maybeSwap                 — ≤ 1 make-before-break swap per cycle (SPEC-R gates:
                                     cooldown → worst incumbent → MIN_DWELL → candidate gates →
                                     hysteresis ≥ +30%); verdicts → viberoxy_swap_total{result}
```

**Canary loop** (`canaryLoop` → `runCanaries`, every 15–30 s ± 20 % jitter):
probe every active/draining slot through its SOCKS listener against
`HEALTH_ENDPOINTS` → `health.ApplyCanary` → environmental failure (≥ 75 % of
paths) trips `viberoxy_env_degraded` and records NOTHING; otherwise per-path
success resets failures / feeds half-open recovery / un-drains, per-path failure
counts toward `WAN_FAIL_THRESHOLD` → `MarkDrainingHealth`.

**One connection:**

```
CONNECT / SOCKS5 request
  → auth: bind policy + PROXY_USERS (407 / RFC 1929) + TargetAllowed (SSRF guard)
  → decideRoute (proxycfg.Router — host-string suffix match)
  → wan:  dialWANFailover — Select(tried) [internal/sched P2C + HRW affinity, atomic reserve]
    + retry.Run (≤ 3 attempts, shared 5 s budget, token bucket); 503 = no candidate, 502 = all dials failed
    direct: directDial (no WAN accounting, TCP_NODELAY + keepalive)
  → relayThroughWAN / directRelay — relayio.Splice, first down byte → TTFB
  → health.ClassifyReason → path.RecordOutcome (window, ejection, viberoxy_conn_outcome_total)
    + path.RecordHealth (TTFB/goodput EWMAs → viberoxy_path_ttfb_seconds)
  → logAccess (status DERIVED: "ok" only when down > 0) + proxy_bytes/latency metrics
```

There is no `TestAll` anywhere (F-21); the speed test is
`TestSpeedWithStability`, and `runCycle` is the cycle entry point.

## State machines

**WAN slot:** `empty → testing → active → draining → empty`

- Draining = no NEW connections; in-flight flows finish, then the slot is
  reaped — or at `DRAIN_MAX` (default 600 s) as the hard stop. The old
  `max(60s, 2×FETCH_INTERVAL)` timer is gone.
- A drain ordered by **health** (`MarkDrainingHealth`) is reversible: canary
  recovery un-drains it (`UnDrainIfRecovered`); a drain ordered by
  **replacement** (`MarkDraining`) never un-drains.
- Make-before-break: `DropAndReplace` tests/starts the candidate on spare
  ports while the old occupant serves; `Swap` is the atomic cutover (old path
  → `Draining`, keeps counters for in-flight handlers), old process stopped
  strictly after.

**Path generation** (`internal/path`): `0 probation (unused) → 1 active →
2 suspect (ejected, not selected) → 3 draining → 4 dead`. Minted per
(re)activation, never reused; handlers reserve/release the exact generation
they selected (F-04 ABA guard). This is the `path` label of the SPEC-M metrics.

## Selection and readiness

**Eligibility** (three passes, decided in `wan.go:Select`):

1. Routable: slot **Active**, path **Active**, under `WAN_FAIL_THRESHOLD`,
   live process, not `Suspect`, not already tried on this connection.
2. Degraded fallback: any untried **active** path even over threshold
   (logged, counted by `viberoxy_selection_last_resort_total`).
3. Last resort: no active path at all → a **draining** slot may take the
   connection (also logged + counted). Draining never attracts traffic while
   an active path exists (F-02).

**Ranking** (F-15/F-05, `internal/sched`): power-of-two-choices over the
peak-EWMA cost `(inflight+1) × max(ttfb, 50 ms) / weight`, weight from
goodput/median × slow-start ramp × state factor; ties are **random**, never
slot order; select+reserve is one atomic step. With a `(client, site)` key
(SOCKS/CONNECT username or client IP × eTLD+1 site key; `NO_AFFINITY_DOMAINS`
opts hosts out) an HRW pick is preferred and spills back to P2C above 3× the
tier's median load. `GetLeastLoaded(Excluding)` survives as a TEST-ONLY
wrapper pinning the legacy ranking — no production caller.

`/readyz` = 200 iff `RoutableCount(DefaultFailThreshold) >= 1`, counting
**ACTIVE routable slots only** (D-03) — a draining-only pool is 503.
`HealthCheckAll` runs every **cycle** (not every `KEEPALIVE_INTERVAL`, whose
meaning changed: it now drives the canary band).

## Key Functions

| Function | File | What it does |
|---|---|---|
| `ParseConfig()` | `internal/proxycfg/config.go` | Read & validate every env knob, return `*proxycfg.Config` (hard-exit on bad values, secrets never echoed) |
| `ParseConfigs(body)` / `ParseSingleErr(raw)` | `internal/proxycfg/parse.go` | Subscription body → configs; one sharelink → config or a specific rejection reason |
| `NewRouter/Decide` | `internal/proxycfg/router.go` | Split-routing decision on the host string |
| `startup(cfg, ctx)` | `main.go` | Fetch/test until WAN_COUNT active, start front-ends/API/metrics/canaries, enter `runLoop` |
| `runCycle(cfg, pool, candidatePool, drainMax)` | `main.go` | One fetch/test/reap/rotate cycle (note: `drainMax`, not `grace`) |
| `runCanaries(cfg, pool, endpoints)` | `main.go` | One canary interval: probe, breaker, per-path record/un-drain/drain |
| `reapCompletedDrains(pool, drainMax)` | `main.go` | Drain reap + `viberoxy_drain_seconds` + reset |
| `rotator.decide/maybeSwap` | `main.go` | SPEC-R verdict (cooldown/dwell/hysteresis/no_candidate) and the single make-before-break swap |
| `TestSpeedWithStability` | `tester.go` | Speed + stability test through a temp xray |
| `NewWANPool(count, basePort)` | `wan.go` | Create N slots, each with a vacant `*path.Path` |
| `Select(SelectOptions)` | `wan.go` | Production selection: 3-pass eligibility → `internal/sched` P2C + HRW affinity, atomic reserve; returns `*path.Path` + release |
| `RoutableCount(threshold)` | `wan.go` | ACTIVE routable slots only (readiness, D-03) |
| `DropAndReplace/Swap/ResetEmpty` | `wan.go` | Make-before-break replacement, atomic cutover, slot reset (retired path → Dead) |
| `DrainExpired` / `UnDrainIfRecovered` | `wan.go` | Drain completion (inflight==0 or DRAIN_MAX) and health-recovery un-drain |
| `HealthCheckAll()` | `wan.go` | Reap slots whose xray died (each cycle) |
| `NewProxyServer/NewSocksServer` | `proxy.go`/`socks.go` | Front-ends; optional router + auth (variadic router arg) |
| `dialWANFailover` | `relay.go` | Bounded dial-stage failover over tried-excluding selection |
| `relayThroughWAN` | `relay.go` | Splice + `ClassifyReason` → window + `viberoxy_conn_outcome_total` + TTFB/goodput sample |
| `logAccess` | `relay.go` | Access log; status derived from bytes actually delivered |
| `BuildXrayConfig/StartXray/StopXray/HealthCheckXray` | `xray.go` (wrappers) | → `internal/xraycfg` (config JSON) and `internal/xrayproc` (process) |
| `Classify` / `ClassifyReason` | `internal/health/health.go` | SPEC-H.1 outcome (+ stable reason for the metric label) |
| `sched.Select` | `internal/sched/sched.go` | Tiered P2C ranking + atomic reservation (+ HRW stickiness via `affinity.Pick`) |
| `retry.Run` | `internal/retry/retry.go` | Dial-stage-only bounded retry; emits `viberoxy_retry_total` events via the installed hook |
| `NewAPIHandler` | `api.go` | Control API (candidates redacted: `raw_sha256`, never `raw`) |
| `NewObservabilityHandler` / `refreshPoolMetrics` | `health.go` | `/metrics`, `/healthz`, `/readyz`; per-path series + env gauge refresh on scrape |
| `NewRegistry`/`NewGauge`/`NewCounter`/`NewHistogram` | `metrics.go` | Hand-rolled Prometheus-text registry (stdlib only) |

## Environment variables (parity with README.md)

| Variable | Default | Notes |
|---|---|---|
| `SUBSCRIBER_URL` | — (required) | `https://` only unless `ALLOW_HTTP_SUBSCRIPTION=true` |
| `ALLOW_HTTP_SUBSCRIPTION` | `false` | Opt in to plain `http://` subscriptions |
| `FETCH_INTERVAL` | `300` (min 30) | Cycle interval **and** rotation cooldown |
| `TEST_TIMEOUT` | `10` (min 3) | Seconds per speed test / canary timeout |
| `DOWNLOAD_SIZE` | `10000000` (min 1000000) | Speed-test payload bytes |
| `DOWNLOAD_ENDPOINT` | `https://speed.cloudflare.com/__down?bytes=` | Empty disables the primary endpoint |
| `DOWNLOAD_FALLBACK` | `https://proof.ovh.net/files/` | Fallback endpoint |
| `WAN_COUNT` | `4` (1–5) | Slots |
| `WAN_BASE_PORT` | `10700` | Slot i listens on `WAN_BASE_PORT+i` (spares above `+WAN_COUNT`) |
| `TEST_BASE_PORT` | `10800` | Shared speed-test port range |
| `PROXY_PORT` | `1080` | CONNECT proxy |
| `SOCKS_PORT` | `0` (off) | SOCKS5; RFC 1929 auth iff `PROXY_USERS` set; UDP ASSOCIATE/BIND → REP `0x07` |
| `METRICS_PORT` | `0` (off) | `/metrics` (Bearer iff `API_TOKEN`), `/healthz`, `/readyz` |
| `API_PORT` | `1980` | Control API |
| `MINIMUM_SPEED` | `5.0` (min 0.1) | Promotion + rotation bar (Mbps) |
| `MAX_TEST_PER_CYCLE` | `20` (1–500) | Tests per cycle while the pool has room; full pool evaluates ≤ 2 NEW (code constant `maxTestPerCycleFull`) |
| `KEEPALIVE_INTERVAL` | `300` (min 10) | Canary base interval, clamped to 15–30 s, ±20 % jitter |
| `WAN_FAIL_THRESHOLD` | `2` (min 1) | Selection cutoff **and** canary drain threshold |
| `STABILITY_PROBES` | `0` (0–5) | Exit-IP probes per passed test (ranking only) |
| `ACCESS_LOG` | `true` | Per-connection access log |
| `ALLOW_DEGRADED_BOOT` | `true` | Serve after the first active WAN |
| `ROUTE_MODE` | `all-proxy` | `all-proxy` \| `proxy-default` \| `direct-default` |
| `DIRECT_DOMAINS` / `PROXY_DOMAINS` | — | Comma-separated suffix lists |
| `DIRECT_LIST_FILE` / `PROXY_LIST_FILE` | — | Newline-separated list files (`#` comments) |
| `XRAY_MUX` | `false` (D-04) | Mux opt-in; auto-suppressed with `flow=xtls-rprx-vision` |
| `LISTEN_ADDR` | `127.0.0.1` | IP literal; non-loopback needs auth or `ALLOW_PUBLIC=true` |
| `PROXY_USERS` | — | `user:password,...` (split at first colon) → CONNECT 407 + SOCKS5 RFC 1929 |
| `API_TOKEN` | — | Bearer for `/api/*` + `/metrics` (no whitespace) |
| `ALLOW_PUBLIC` | `false` | Non-loopback bind without auth |
| `ALLOW_PRIVATE_TARGETS` | `false` | SSRF guard opt-in |
| `NO_AFFINITY_DOMAINS` | — | Comma/suffix list; hosts exempt from session stickiness |
| `DRAIN_MAX` | `600` | Seconds; invalid/unset → default + warning (`drainMaxFromEnv`) |
| `HEALTH_ENDPOINTS` | gstatic `generate_204` + cloudflare `cdn-cgi/trace` | Canary endpoints; ipify never used for health |

Code-only knobs (no env yet): `maxTestPerCycleFull = 2`,
`rotationHysteresis = 0.30`, `rotationMinDwell = 10m`, retry policy
(3 attempts / 5 s / 10 % ratio / burst 10), scheduler constants (TTFB floor
50 ms, ramp window 60 s, weight clamp 0.25–4, affinity spill factor 3×),
all SPEC-H thresholds (window, ejection rules, backoff, 75 % env breaker).

## Metrics (SPEC-M)

All in `metrics.go`, Prometheus text; `path` = generation ID, `slot` = WAN index.
`viberoxy_path_state{path,slot}` (0–4), `viberoxy_path_inflight{path,slot}`,
`viberoxy_path_ttfb_seconds`, `viberoxy_path_goodput_bps{path,slot}`,
`viberoxy_conn_outcome_total{path,outcome,reason}`,
`viberoxy_path_ejections_total{path,reason}`, `viberoxy_retry_total{stage,outcome}`,
`viberoxy_swap_total{result}`, `viberoxy_env_degraded`,
`viberoxy_selection_last_resort_total`, `viberoxy_drain_seconds`,
`viberoxy_session_reset_on_drain_total` (MUST stay 0 — tripwire test),
plus the historical `wans_active`, `wan_speed_mbps{index}`,
`wan_stability{index}`, `proxy_connections_total{wan,proto}`,
`proxy_bytes_total{wan,direction}`, `proxy_latency_seconds`,
`test_duration_seconds`, `build_info{version}`.

## Verification suites (integrity / chaos)

- `integrity_test.go` (normal suite; `-short` shrinks transfers): **T-INT-01**
  cross-talk-under-chaos — N fake clients × 200 conns through the REAL
  front-end stack against `testutil.FakeWAN` backends under continuous seeded
  faults (die/revive/mode flips, health-drain, ejection, one make-before-break
  swap); every response must be the client's own payload (crc-verified, one
  known WAN tag) or a clean protocol error — never another client's bytes.
  Iterations: 2 in the normal suite, 25 behind `-tags chaos`
  (`CHAOS_ITERATIONS` overrides). **T-INT-02**: 256 MiB SHA-256 stream
  survives a mid-stream swap of its own slot.
- `chaos_test.go` (`//go:build chaos`): **T-CHAOS-01..09** with asserted
  budgets (2 s selection exit, 20 s window ejection, traffic-share drops,
  env_degraded holds, zero churn on bad subscriptions, ≤ 1 re-admit/60 s,
  swap-under-load zero failures, mid-stream swap hash).
- Run: `go test -tags chaos -race -count=1 -run 'TestChaos|TestIntegrity' -timeout 15m .`
  (also the CI `chaos` job). Pitfalls baked into the suite: FakeWAN
  `Blackhole` wedges handlers until idle timeout (chaos tests use
  `AcceptClose` as the black-hole flavor); T-CHAOS-02/04 pin the SPEC-H.3
  window by raising `WanFailThreshold`; the T-INT swap is ordered against
  `relay.go`'s by-design unlocked `ServicePort` reads via metric-mutex covers
  (see the file headers before touching either).

## Conventions

- **Zero dependencies:** Go stdlib only. No external packages.
- **Error handling:** return errors; log at the call site with
  `slog.Warn`/`slog.Error`. Never `log.Println`.
- **Logging:** `slog` key-value pairs. Secrets are never echoed in errors.
- **Env config:** all configuration via env vars, validated in
  `proxycfg.ParseConfig` — bad value = hard exit; enforcement points re-read
  the env at request/bind time (dual-read pattern).
- **Thread safety:** `sync.Mutex` per WAN slot; `sync/atomic` for path
  inflight/state; the metric registry locks per metric; hooks
  (`retry.SetEventHook`, `path.SetObserver`) are installed once from
  `metrics.go:init` so internal packages never import the registry.
- **Tests:** `go test -race -shuffle=on -count=1 ./...`; red suite via
  `bash scripts/red_gate.sh` (empty allow list — any FAIL/SKIP exits 1,
  currently 11/11 PASS). TDD markers are `T-XXX-nn` in test names.
- **Host pitfall:** back-to-back `-race` runs can exhaust the loopback
  ephemeral port range; unrelated tests then fail with
  `can't assign requested address`. Wait ~60 s and re-run before believing
  such a failure.
- **Docs:** `README.md` is the source of truth for user-facing behavior
  (env table, breaking changes, limitations). `AGENTS.md` writes require
  interactive approval — put drafts in `agents_temp.md`.
