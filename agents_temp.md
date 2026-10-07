# Agent notes (temporary — consolidate into AGENTS.md at campaign end)

The campaign constraint forbids subagents from writing AGENTS.md directly, so
layout changes are recorded here until someone folds them into AGENTS.md.

## task-1-1: config/parsing layer extracted to `internal/proxycfg` (behavior-identical)

Moved into `internal/proxycfg/`:

| File | Contents (was in package `main`) |
|---|---|
| `config.go` | `Config` struct, `(*Config).MaxTestPerCycleVal`, `ParseConfig()` (was `parseConfig()` in `main.go`) |
| `types.go` | `ProxyConfig` struct (was `parser.go`) |
| `parse.go` | `ParseConfigs`, `ParseSingle`, `IsXraySupported`, base64/URI helpers (was `parser.go`) |
| `router.go` | `Router`, `Route`, `RouteMode`, `NewRouter`, `ParseRouteMode`, suffix-list helpers (was `router.go`) |
| `config_test.go` | the `TestParseConfig_*` env-config tests ported from `main_test.go` |
| `parse_test.go` | was `parser_test.go` |
| `router_test.go` | was `router_test.go` |

Why `router.go` moved too: `Config.Router *Router` and `parseConfig` build the
Router from `ROUTE_MODE`/`*_DOMAINS`/`*_LIST_FILE`; `package main` cannot be
imported by `internal/proxycfg`, so the `Router` type had to live with the
config. Root callers only ever used the exported API (`NewRouter`, `Decide`,
`Route*` constants) and now qualify it as `proxycfg.…`.

Call-site changes in package `main` (all mechanical):

- `main()`: `cfg, err := proxycfg.ParseConfig()`
- `xray.go`: `base64Decode` → `proxycfg.Base64Decode` (exported because
  `extractSIP002`/`extractVMessRaw` use it)
- signatures/literals: `*Config` → `*proxycfg.Config`, `*ProxyConfig` →
  `*proxycfg.ProxyConfig`, `*Router` → `*proxycfg.Router`, `RouteWAN`/`RouteDirect`/
  `RouteProxyDefault`/… → `proxycfg.…`
- `metrics_test.go` still calls the env parser (its `TestParseConfig_Metrics*`
  tests stayed put) via `proxycfg.ParseConfig()`; `setenv`/`unsetenv` remain in
  `main_test.go` for those tests and are duplicated in `config_test.go`.

Stale AGENTS.md pointers (for whoever consolidates): "Change env vars or
startup flow → `main.go` — `parseConfig`", "Add a new proxy protocol parser →
`parser.go` — `ParseSingle` switch", "Change split-routing rules → `router.go`",
the Directory Map rows for `parser.go`/`router.go`, and the Key Functions rows
for `parseConfig()` / `ParseConfigs()` / `ParseSingle()`.

Test-failure strings in the ported tests still say `parseConfig() error: %v`
(intentionally left byte-identical to the pre-move suite; cosmetic only).

## task-1-2: xray process lifecycle extracted to `internal/xrayproc` (F-06)

Two commits on `task-1-2-xrayproc`:

1. `refactor(xrayproc): extract process lifecycle (behavior-identical)` — moved
   `StartXray`/`StopXray`/`HealthCheckXray`'s process handling out of `xray.go`
   into `internal/xrayproc/proc.go`. `BuildXrayConfig` and the `extract*`
   helpers stay in `xray.go` (a later task owns them). `xray.go` keeps thin
   wrappers with unchanged names/signature shapes, so `main.go`, `tester.go`
   and `api.go` needed no edits.
2. `fix(xrayproc): Wait()-based liveness; crashed xray detected (F-06); RT-04 green`
   — each started child now gets one goroutine that runs `cmd.Wait()`; `Handle`
   exposes `Exited <-chan struct{}` (closed after the reap), `Alive()` selects
   on it, and `ExitReason()` carries the Wait error. This reaps zombies instead
   of answering a `Signal(0)` probe for an exited-but-unreaped child.

Layout / API changes:

| Was | Now |
|---|---|
| `WANSlot.Cmd *exec.Cmd` | `WANSlot.Cmd *xrayproc.Handle` |
| `ReplacementStarter … (*exec.Cmd, string, error)` | `… (*xrayproc.Handle, string, error)` |
| `WANPool.SetActive(index, *exec.Cmd, path)` | `SetActive(index, *xrayproc.Handle, path)` |
| `HealthCheckXray(*exec.Cmd) bool` | `HealthCheckXray(*xrayproc.Handle) bool` (delegates to `Handle.Alive`) |

Test-fixture plumbing: fixtures that used to assign a raw `*exec.Cmd` now wrap
it (`xrayproc.Wrap(cmd, path)`), and tests no longer call `cmd.Wait()` on a
wrapped child — the handle owns `Wait`, tests wait on `handle.Exited`
(two double-`Wait` races removed in `xray_test.go` and `wan_test.go`).

**Deviation (flag for the task that owns `metrics_test.go`):** `WANSlot.Cmd`
changed type, so `metrics_test.go`'s `/readyz` fixture had to change
mechanically (3 lines: start into a local `sleepCmd`, `defer` its `Kill`, then
`pool.Slots[0].Cmd = xrayproc.Wrap(sleepCmd, "")`). No assertions touched.

`scripts/red_gate.sh`: `TestRed_RT04_HealthCheckDetectsExitedProcess` removed
from `DEFAULT_ALLOW` (it now passes); allow list is 10 entries, gate exit 0.

Stale AGENTS.md pointers (for whoever consolidates): "Health-check / stop a
running xray → `xray.go`" (now `internal/xrayproc`), and any Directory Map row
naming `StartXray`/`StopXray`/`HealthCheckXray` as living in `xray.go`.

## task-1-3: xray config generation extracted to `internal/xraycfg` (F-11, F-17-builder, D-04)

Three commits on `task-1-3-xraycfg`:

1. `refactor(xraycfg): extract xray config generation (behavior-identical)` —
   `XrayConfig`/`OutboundConfig`/`StreamSettings` types, `BuildXrayConfig`,
   `buildOutbound`, `buildStreamSettings`, `getXrayProtocol`, `extract*` and
   `marshalRaw`/`getInt` moved verbatim from `xray.go` into
   `internal/xraycfg/{types.go,build.go}`. `xray.go` keeps a thin
   `BuildXrayConfig` wrapper plus a `type XrayConfig = xraycfg.XrayConfig`
   alias, so `red_test.go` and most of `xray_test.go` needed no edits; the
   tests of unexported helpers (`TestExtractSIP002`, `TestExtractSocksParams`,
   `TestStreamSettings_*`) moved with their targets into
   `internal/xraycfg/build_test.go`.
2. `fix(xraycfg): no mux with flow (F-11), error-returning builders (F-17),
   test/prod mux parity; RT-05 green, red gate shrinks to 9` —
   * **F-11**: a non-empty `flow` (e.g. `xtls-rprx-vision`) now suppresses the
     `mux` block on the outbound even when the caller requests mux on, next to
     the existing freedom-fallback and socks5 no-mux rules.
   * **F-17 (builder half)**: builders return `(..., error)` end to end —
     undecodable/empty vmess JSON (no more zero-UUID outbound), undecodable ss
     userinfo (no more `method: none`), xhttp missing path/host/mode (mirrors
     proxycfg's parse-time `checkXHTTP`). `buildStreamSettings` gained a `mode`
     parameter (`extractVLessParams`/`extractTrojanParams`/vmess raw now plumb
     `mode`).
   * **test/prod mux parity**: `tester.go` starts its speed-test xray through
     `startTestXray(cfg, testPort, xrayMuxForRun())` — `xrayMuxForRun()` in
     `xray.go` re-parses `XRAY_MUX` exactly like `proxycfg.ParseConfig`, so the
     temp xray always matches `cfg.XrayMux`. `TestSpeedTestXrayMuxMatchesProduction`
     (xray_test.go) pins this across XRAY_MUX unset/true/false and also checks
     byte-identical renders plus the T-XRAY-01 golden.
   * **T-XRAY-01 golden tests**: `internal/xraycfg/golden_test.go` renders
     ss, vmess+ws, vless ws/grpc/tcp/reality+vision, trojan and socks5 into
     `internal/xraycfg/testdata/<case>.mux-{on,off}.json`, asserting structure,
     no-mux-with-flow, and that mux-suppressed cases render byte-identical
     with the flag on or off. Regenerate with
     `go test ./internal/xraycfg -run TestBuildXrayConfig_Golden -update`.
   * `scripts/red_gate.sh`: `TestRed_RT05_NoMuxWithVisionFlow` removed from
     `DEFAULT_ALLOW` (it passes now); allow list is 9 entries, gate exit 0.
3. `feat(proxycfg): D-04 — XRAY_MUX defaults to off` —
   `proxycfg.ParseConfig` now defaults `XrayMux` to **false**
   (`XRAY_MUX=true` still opts in); `config_test.go`'s default assertion
   flipped accordingly. **`xrayMuxForRun()`'s fallback default flipped in the
   same commit** — it is a deliberate mirror of proxycfg's default and must
   always be changed together with it, or speed tests and production diverge
   (the parity test fails loudly if they ever do). `BuildXrayConfig`'s
   omitted-argument default stays `true`: production and the tester both pass
   the flag explicitly, and the omitted default is only used by tests.
   README.md/AGENTS.md are frozen for this campaign, so the README table row
   (`XRAY_MUX` default `true`) is stale until consolidation.

Stale AGENTS.md pointers (for whoever consolidates): any Directory Map /
Key Functions row still naming the config-generation types, `BuildXrayConfig`
or the `extract*` helpers as living in `xray.go` — they are in
`internal/xraycfg` now; `xray.go` is a thin wrapper around them plus the
process-lifecycle wrappers.

## task-1-5: subscription fetching extracted to `internal/subs` (F-18)

New package `internal/subs/` (`fetch.go` + `fetch_test.go`, stdlib only) owns
what `main.go:fetchSubscription` used to do inline:

| Behavior | Where |
|---|---|
| body cap — `io.LimitReader(resp.Body, MaxBodyBytes+1)`, `MaxBodyBytes = 8 MiB`; over the cap is a `KindOversized` error, never an unbounded `io.ReadAll` | `subs.Fetch` |
| https-only scheme policy; plain `http://` needs `ALLOW_HTTP_SUBSCRIPTION=true` | `subs.Fetch` (reads the env via `subs.AllowHTTPFromEnv()`, `Options.AllowHTTP` forces it) **and** `proxycfg.ParseConfig` (rejects an http `SUBSCRIBER_URL` at startup unless opted in) |
| conditional GET — `If-None-Match` / `If-Modified-Since` replayed from `Options.ETag` / `Options.LastModified`; a 304 is `Result{NotModified: true}` with a **nil error** (first-class result) | `subs.Fetch` |
| empty/whitespace body → `KindEmptyBody` error | `subs.Fetch` |
| distinguishable kinds — `KindOK`, `KindTransport`, `KindBadScheme`, `KindOversized`, `KindStatus`, `KindEmptyBody`, `KindNotModified` (`KindOf(err)` / `Outcome(res, err)`) | `internal/subs` |

`main.go` keeps its one job: `fetchSubscription(url) []*proxycfg.ProxyConfig`
logs and returns what the two call sites need. It now holds a mutex-guarded
`fetchSubscriptionState` (url + ETag + Last-Modified + last parsed configs):
validators are only replayed to the URL they came from, an error or a body
that parses to zero configs returns `nil` (startup keeps retrying, a running
cycle keeps its pool), and a 304 returns the configs already parsed. New env
var: `ALLOW_HTTP_SUBSCRIPTION` (bool, default false) — field
`Config.AllowHTTPSubscription`, parsed and enforced in `ParseConfig`.

`README.md` env table not updated (out of scope for this task): someone
should add `ALLOW_HTTP_SUBSCRIPTION` when consolidating into `AGENTS.md`.

**Deviation (flag for whoever owns `main_test.go`):** `main_test.go` gained an
`init()` that sets `ALLOW_HTTP_SUBSCRIPTION=true`, because its integration
tests drive `fetchSubscription` / `startup()` / `runCycle()` against
plain-http `httptest` servers. The policy itself is covered both ways in
`internal/subs/fetch_test.go` (env unset → rejected, env set → allowed) and in
`internal/proxycfg/config_test.go` (default false, opt-in, invalid value,
http `SUBSCRIBER_URL` rejected by default). `main_test.go` also gained two
caller tests: `TestFetchSubscription_ConditionalGetKeepsConfigs` (304 replay)
and `TestFetchSubscription_GarbageBodyKeepsPrevious`.

## task-1-6: F-14 security hardening (loopback bind, proxy/API auth, credential redaction, SSRF guard)

One commit on `task-1-6-auth`: `feat(auth): loopback default bind, proxy/API auth, credential redaction (F-14)`.

New package `internal/auth/` (`auth.go` + `auth_test.go`, stdlib only) owns the primitives:

| Behavior | Where |
|---|---|
| `PROXY_USERS` parsing (`user:password,...`, split at the FIRST colon so passwords may contain colons; errors never echo the value) | `auth.ParseUsers` |
| constant-time credential check (`crypto/subtle`, looped without early exit) | `auth.CheckUsers` |
| `Proxy-Authorization: Basic` parsing (case-insensitive scheme) | `auth.ParseBasic` |
| `Authorization: Bearer` check + gate middleware (403 + `WWW-Authenticate`) | `auth.CheckBearer`, `auth.RequireBearer` |
| bind policy: loopback always allowed; non-loopback needs `PROXY_USERS` or `API_TOKEN`, else `ALLOW_PUBLIC=true` | `auth.EnsureBindAllowed` / `BindAllowed` / `BindAllowedFromEnv` |
| SSRF guard: loopback / link-local (incl. 169.254.169.254) / RFC1918+ULA / CGNAT / unspecified blocked unless `ALLOW_PRIVATE_TARGETS=true`; IPv4 shorthand normalised (`127.1`, `2130706433`, `0177.0.0.1`, `0x7f000001`); localhost-family names blocked; hostnames pass WITHOUT DNS resolution (documented: targets dial remotely through the WAN, a local resolve would add latency and a new failure mode) | `auth.TargetAllowed` |

New env vars — all parsed and hard-exit validated in `proxycfg.ParseConfig`, then re-read at
request/bind time by the enforcement points (the same dual-read pattern as
`subs.AllowHTTPFromEnv` for `ALLOW_HTTP_SUBSCRIPTION`):

- `LISTEN_ADDR` (IP literal, default `127.0.0.1`): bind host for the proxy, SOCKS5, API and
  metrics listeners. Non-loopback is refused at startup unless `PROXY_USERS`/`API_TOKEN` is set
  or `ALLOW_PUBLIC=true`.
- `PROXY_USERS`: comma-separated `user:password` entries; enables CONNECT 407 (Basic) and
  SOCKS5 RFC 1929 (method `0x02`, failure status `0x01`, no-auth-only greeting → `0xFF`).
  Unset keeps the historical no-auth paths byte-for-byte.
- `API_TOKEN`: bearer token gating `/api/*` (api.go) and `/metrics` only (main.go `gateMetrics`;
  `/healthz` + `/readyz` stay open for probes).
- `ALLOW_PUBLIC`, `ALLOW_PRIVATE_TARGETS` (bools, default false).

Front-end wiring: `ProxyServer`/`SocksServer` gained a `listenAddr` field defaulting to
`auth.LoopbackHost` (constructor-level defense) which `main.go` overrides via `listenHost(cfg)`
(`""` in a hand-built Config still means loopback, never `:port`). `Start`/`Listen` re-check
`auth.EnsureBindAllowed` before binding (defense in depth behind ParseConfig's hard exit), and
both front-ends run `auth.TargetAllowed` before any dial (CONNECT → 403, SOCKS5 → REP `0x02`).

Credential redaction: `GET /api/viberoxy/candidates` no longer returns `raw`; entries expose
`{name, server, protocol, port, raw_sha256, speed_mbps, error?}` (`raw_sha256` = hex sha256 of
the Raw link so callers can still correlate). Audit of the rest of api.go: the other endpoints
expose only `WANSlotInfo` (no Config/Raw) and fixed strings — no other credential path found.

Tests: `internal/auth/auth_test.go` (primitives, both ways),
`internal/proxycfg/listen_auth_test.go` (+ F-14 defaults added to `TestParseConfig_Defaults`),
root `security_test.go` (bind default, bind refusal/opt-in, 407 + authenticated tunnel, RFC 1929
reject/accept, API 403/200, metrics 403 with open probes, candidates redaction against a
UUID/password fixture, SSRF guard both ways on both front-ends). `security_test.go` has an
`init()` setting `ALLOW_PRIVATE_TARGETS=true` for the whole test binary (existing proxy/socks
tests target 127.0.0.1 echo servers) — the same pattern as `main_test.go`'s
`ALLOW_HTTP_SUBSCRIPTION` init; each guard test still pins both directions explicitly. Red suite
untouched: 9 FAIL / 2 PASS / 0 SKIP, `scripts/red_gate.sh` exit 0.

Stale README/AGENTS rows for whoever consolidates: the `SOCKS_PORT` row ("No auth (RFC 1929 not
implemented)") is now conditional on `PROXY_USERS`, and the README env table needs `LISTEN_ADDR`,
`PROXY_USERS`, `API_TOKEN`, `ALLOW_PUBLIC`, `ALLOW_PRIVATE_TARGETS` plus a "loopback by default"
note on the `PROXY_PORT`/`SOCKS_PORT`/`API_PORT`/`METRICS_PORT` bindings.

## task-2-1: `internal/path` — Path-owned accounting (F-04 root fix, RT-03 green)

New package `internal/path`. A `Path` is one xray process lifetime: `ID` (process-wide
monotonic, never reused), `Cfg`, `Proc`, `Port`, `Slot`, `Inflight atomic.Int64`,
`State atomic.Int32` (`Probation|Active|Suspect|Draining|Dead` — only Active/Draining/Dead are
assigned today), `CreatedAt`, plus placeholder `ewmaTTFB`/`goodputBps` fields documented as
"populated by later health/scheduler tasks". Methods: `Reserve`/`Release` (release ALWAYS
targets the receiver — that is the ABA guard), `Conns` (pool-facing read, clamps at 0),
`RecordFailure`/`RecordSuccess`/`ConsecutiveFails`, `GetState`/`SetState`, `Alive`/`Stop` (via
`Proc`); all methods are nil-safe.

| Was | Now |
|---|---|
| `WANSlot.ConnCount int64` | gone — lives in `Path.Inflight` |
| `WANSlot.ConsecutiveFails int64` | gone — lives in `Path` (unexported; `RecordFailure`/`RecordSuccess`/`ConsecutiveFails` methods) |
| — | `WANSlot.Current *atomic.Pointer[path.Path]` (never nil: `NewWANPool`/`ResetEmpty` install a **vacant** path with a fresh ID; `SetActive` installs a new occupant path; `ResetEmpty` marks the old one `Dead`) |
| `WANPool.IncConnCount(i)` / `DecConnCount(i)` | deleted — handlers call `Reserve`/`Release` on the `*Path` they selected |
| `WANPool.GetLeastLoaded(...) int` | `GetLeastLoaded(...) *path.Path` (`nil` = no active/draining slot); selection rule byte-for-byte unchanged |
| `WANPool.RecordFailure/RecordSuccess/SlotConsecutiveFails(i)` | unchanged signatures; they resolve the slot's **current** path (production probes only target active/draining slots, which always have a path) |
| `relay.beginWAN/endWAN/dialWAN/relayThroughWAN(wanIndex int, ...)` | take the held `*path.Path`; dial failures and relay success credit that held path, never the slot's current occupant |

`api.go`'s `WANSlotInfo` schema is unchanged (`conns`/`consecutive_fails` now read the current
path). `dialWAN` still reads `ServicePort` from the slot (fixed per slot; tests reconfigure it
in place after construction).

TDD: `TestRed_RT03_ConnCountNeverNegative` passes (plumbing adapted, `t.Fatal*`/`t.Error*`
lines byte-identical to HEAD); removed from `DEFAULT_ALLOW` in `scripts/red_gate.sh` (gate now
6 entries, exit 0; full red suite 6 FAIL / 5 PASS / 0 SKIP). **T-PATH-01** is
`TestPath_TPATH01_OldPathCannotMutateReplacement` in `wan_test.go` — it drives the real guard
(`relay.beginWAN`/`endWAN` across reset + re-occupy), which lives in package main, so it cannot
live in `internal/path`; `internal/path/path_test.go` additionally pins clamped visibility, ID
monotonicity, concurrent `Reserve`/`Release` under `-race`, process `Alive`/`Stop`, and the
object-level old-vs-replacement independence.

Test plumbing added: `slotOf`, `slotInflight`, `setSlotInflight`, `setFails` helpers in
`wan_test.go` (they replace the direct `slot.ConnCount`/`slot.ConsecutiveFails` pokes that the
compiler also forced out of `api_test.go`/`proxy_test.go`).

Stale pointers for whoever consolidates: docs/rows referencing `WANSlot.ConnCount`,
`WANSlot.ConsecutiveFails`, `IncConnCount`/`DecConnCount`, or an index-returning
`GetLeastLoaded`; the red-gate allow list is now 6 entries (RT-03 fixed by this task).

## task-3-2: candidate pool extracted to `internal/cands` (F-12)

Two commits on `task-3-2-cands`:

1. `refactor(cands): extract candidate pool (behavior-identical)` —
   `candidate.go` → `internal/cands/pool.go`, `candidate_test.go` →
   `internal/cands/pool_test.go` (the test file is byte-identical to the
   old one modulo `package cands`, `TestResult`→`Entry`, and the
   constructor rename). The type is now `cands.Pool` (`cands.NewPool(maxLen)`),
   storing `cands.Entry` — a mirror of main's `TestResult`
   (Config/Speed/StabilityScore/Error) because the tester stays in
   `package main` and `internal/cands` cannot import it. `runCycle`
   converts with `toCands(results)`; `Config` is shared by pointer, so
   identity assertions in the drop-and-replace tests still hold.
2. `fix(cands): dedupe by Raw, TTL aging, Best(exclude) (F-12); RT-09 green` —
   dedupe by `Config.Raw` (newest wins), `Entry.TestedAt` + `DefaultTTL`
   (30 min; stale entries purged lazily in Update/Best/List/Len), `Best`
   takes an exclusion predicate over `*proxycfg.ProxyConfig`, and
   `DropAndReplace` now excludes both the just-dropped config and any
   server:port that is active/draining (same rule as runCycle's
   `HasServerPort` dedupe). Per-server cap: `DefaultMaxPerServer = 5`
   entries per server:port (fastest kept) so one flaky upstream cannot
   flood the pool.

Call-site changes in package `main` (all mechanical): `*CandidatePool` →
`*cands.Pool`, `NewCandidatePool(...)` → `cands.NewPool(...)`, pool-entry
literals `[]*TestResult` → `[]*cands.Entry` in tests. `red_test.go` kept
its `t.Fatal*`/`t.Error*` lines byte-identical (grep diff vs baseline 8858e58
empty); only plumbing changed.

New tests: `internal/cands/pool_test.go` gained the T-CAND-01/02/03 units
(TTL aging with explicit `TestedAt` + injected clock, `Best(exclude)`,
per-server cap) and the RT-09 dedupe unit; `api_test.go` gained
`TestWANPoolDropAndReplace_SkipsActiveAndDroppedConfigs` (F-12 part 2).
`scripts/red_gate.sh`: `TestRed_RT09_CandidatePoolDedupes` removed from
`DEFAULT_ALLOW` (it passes now); allow list is 5 entries, gate exit 0;
full red suite 5 FAIL / 6 PASS / 0 SKIP.

Stale pointers for whoever consolidates: the AGENTS.md/README Directory Map
row "`candidate.go` — candidate pool of tested configs" (now
`internal/cands/pool.go`, package `cands`).

## task-2-6: bounded dial-stage failover (F-08 stage 1, RT-08 green)

One commit on `task-2-6-retry`: `feat(retry): bounded dial-stage failover (F-08 stage 1); RT-08 green`.

New package `internal/retry/` (`retry.go` + `retry_test.go`, stdlib only):

| Piece | Behavior |
|---|---|
| `retry.New(Options{MaxAttempts, Budget, RetryRatio, Burst, Now})` | defaults: **3 total attempts (initial + max 2 extra)**, **5s shared dial budget**, per-request credit **0.10 tokens** (<=10% of requests), burst 10; non-positive option = default (`MaxAttempts: 1` disables retries) |
| `retry.Bucket` | token-bucket retry budget (T-RETRY-05): every admitted request deposits `RetryRatio` tokens clamped to `Burst`, every retry debits 1; below one token the caller **fails fast on the first dial error**, so a correlated-failure storm costs <= `Burst + N*ratio` retries over N connections |
| `retry.Run(ctx, policy, next, dial)` | the bounded loop: candidate from `next` (caller owns exclusion), one dial-stage attempt per candidate; stops at attempt cap / shared deadline / no candidate / empty bucket. Every attempt's ctx carries the budget deadline (a single hanging dial cannot blow the 5s). `ErrNoCandidate` distinguishes "no path eligible" (503) from "all dials failed" (502 / REP 0x01) |
| safety (T-RETRY-03/04) | dial-stage ONLY: a successful dial is terminal — `Run` returns the conn untouched, does not even request another candidate, and exposes no post-dial/replay API. Pinned by `TestRetry_TRETRY0304_NeverSeesPostDialConnections` + package doc |

Wiring (stage 1):

- `relay.go`: `wanRelay.Retry *retry.Policy` (both front-end constructors install
  a default; hand-built `&wanRelay{pool: p}` falls back to the shared
  `defaultDialRetry`) and `dialWANFailover(ctx, targetHost, start, proto)` — holds
  the per-connection `tried` set; `next()` = `GetLeastLoadedExcluding(tried,
  WanFailThreshold)` marking each path it hands out; each attempt wraps the
  existing `beginWAN`/`dialWAN` pair and releases via `endWAN` when that dial
  fails (failed paths never keep a reservation). On success the caller receives
  `(conn, wanPath)` and keeps its original `defer endWAN(wanPath)` contract.
- `wan.go`: added `GetLeastLoadedExcluding(tried, thresholds...)`; `GetLeastLoaded`
  now delegates with a nil map. Selection rule byte-for-byte unchanged — both
  passes (routable + degraded fallback) merely skip tried paths. No drain/health/
  state changes.
- `proxy.go` / `socks.go`: call sites switched to `dialWANFailover`. CONNECT keeps
  503 on `retry.ErrNoCandidate` and 502 on exhausted attempts; SOCKS5 keeps REP
  `0x01` for both. Direct-route dials never retry through WANs.
- `scripts/red_gate.sh`: `TestRed_RT08_FailoverOnDialFailure` removed from
  `DEFAULT_ALLOW` — allow list **5 entries, gate exit 0**; full red suite
  **5 FAIL / 6 PASS / 0 SKIP** (RT-03..08 green). `red_test.go` untouched:
  RT-08's assertion-line grep diff vs HEAD is empty.

TDD: T-RETRY-01 (refused -> transparent success on the healthy path, real
sockets), T-RETRY-02 (attempt cap + budget, fake clock + ctx-deadline check),
T-RETRY-05 (storm bound + fail-fast after bucket drain) were red with the retry
loop stubbed out, green after implementation. Mutation sanity: disabling the
loop fails `TestRed_RT08_FailoverOnDialFailure` and
`TestRetry_TRETRY01_RefusedFailsOverToHealthyPath`; restoring turns both green.

**Note (for a later config task):** the policy is configurable through
`retry.Options` in code; there is no env var yet — plumbing one would touch
`proxycfg.ParseConfig` + `main.go`, which are outside this task's scope guard.

## task-2-23: health subsystem — passive outcomes + canaries + global breaker (F-01, F-07)

Three commits on `task-2-23-health`:

1. `feat(health): passive outcome window + ejection + backoff (F-01)` — new
   `internal/health` (`health.go` classification + `state.go` window/eject/
   backoff) + `internal/path` state embedding + `relay.go` outcome hooks +
   `wan.go` Suspect exclusion + `scripts/red_gate.sh`.
2. `feat(health): canaries + global breaker (F-07)` — `internal/health/canary.go`
   (endpoints, jitter, breaker) + T-HLT-03/04.
3. `refactor(main): keepalive loop -> health canary loop` — `main.go` wiring +
   `canary_test.go` (knob mapping + environmental-failure wiring test) + this
   section.

New package `internal/health` (pure policy; no knowledge of slots/paths):

| Piece | Behavior |
|---|---|
| `health.Classify(Result{Up,Down,Err,DialErr,Duration,TFirst})` | SPEC-H.1: `OK` iff `down > 0`; `HARD_FAIL` on dial error, `up > 0 && down == 0` at close (both spec clauses: the timed `T_FIRST` flavor — default 8s — and the close flavor — no upstream byte ever arrived); `NEUTRAL` (`up == 0 && down == 0`, client abort) is ignored |
| `health.State` (zero value ready, all methods take explicit `now` — **the injected clock**) | SPEC-H.2 sliding window (60s or 30 outcomes, smaller bound; only OK/HARD_FAIL recorded); SPEC-H.3 ejection: **>= 3 consecutive HARD_FAILs across >= 3 distinct destination HOSTS** (distinctness guard, T-HLT-06) **OR >= 8 outcomes at failRatio >= 50%** (unguarded on purpose — RT-02's dead WAN fails everything to ONE destination); SPEC-H.4 backoff `30s * 2^ejectCount` capped 10min, `ejectCount` decays one step per 10min of health; SPEC-H.5 canary streak: successes only count after the backoff elapsed, 2 consecutive => re-admit, any failure resets the streak |
| `health.ApplyCanary([]PathCanary)` | SPEC-H.6: >=75% of paths failing canaries in the SAME interval => `degraded=true` and an **empty failing list** (nothing recorded/ejected/drained — T-HLT-03/T-CHAOS-05); below that the failing path indices come back for per-path handling |
| `health.EndpointsFromEnv` / `JitterInterval` / `PathOK` | `HEALTH_ENDPOINTS` parsing (default gstatic `generate_204` + cloudflare `cdn-cgi/trace`), 15–30s base ±20% jitter (injected `*rand.Rand`), "path fails only if ALL endpoints failed" |
| `health.SetEnvDegraded/EnvDegraded` | process-wide `env_degraded` gauge (atomic) |

Wiring:

- `internal/path.Path` embeds one `health.State` per generation (state dies
  with the generation) and mirrors decisions onto lifecycle state:
  `RecordOutcome(now, dest, outcome)` (eject => `Active -> Suspect`, never
  overwrites `Draining`/`Dead`) and `NoteCanary(now, ok)` (half-open
  re-admission => `Suspect -> Active`). The placeholder `ewmaTTFB` /
  `goodputBps` fields are now populated (`RecordHealth`, alpha 0.25) from the
  relay's first down byte + goodput; getters `TTFB()`/`GoodputBps()` added.
- `relay.go`: `relayThroughWAN` classifies every relay end and calls
  `RecordOutcome` — **success is credited only when `down > 0`** (the F-01
  minimal fix; a splice error with bytes delivered is still OK);
  `dialWAN` records its dial failure as a `HARD_FAIL` outcome (counter +
  window, exactly one increment as before).
- `wan.go`: `GetLeastLoadedExcluding`'s routable pass also skips
  `path.Suspect` (like an over-threshold path); the degraded fallback is
  untouched (still no blackhole, and `Draining` selection semantics are
  untouched — RT-01 still fails, its rule belongs to a later task).
- `main.go`: `keepaliveLoop`/`probeAllWANs` **replaced** by `canaryLoop` +
  `runCanaries` + `canaryBaseInterval` + `canaryProbe` (per endpoint through
  the slot's SOCKS5 via the existing `probeRoundTrip`, first success wins).

**Env knob mapping (as required):**

- `KEEPALIVE_INTERVAL` now drives the **canary** interval:
  `canaryBaseInterval(keepaliveSeconds)` clamps it into SPEC-H.5's 15–30s
  band (configured default 300 => 30s base; config minimum 10 => 15s), then
  ±20% jitter is applied per tick. It no longer means "probe every 300s".
- `WAN_FAIL_THRESHOLD` keeps both of its meanings: (a) selection's
  over-threshold cutoff (`GetLeastLoaded*`, unchanged), and (b) the number
  of **canary** failures before `runCanaries` marks the slot `Draining` —
  the exact spot the old `probeAllWANs` did it ("where the current code
  would"). Passive ejection thresholds (3-distinct / 8-at-50%, backoff
  schedule) are fixed by SPEC-H.3/H.4 and are **not** env-tunable.
- `HEALTH_ENDPOINTS` (new, read by `internal/health.EndpointsFromEnv`):
  comma/whitespace-separated URLs; unset/blank => the two built-in
  defaults. `ProbeWANURL` (ipify) is now reachable only through
  `probeExitIP` — exit-IP reporting after a passing canary, never health
  (F-07's SPOF fix). `ProbeWAN` itself is now only exercised by its tests.

**Red suite / gate:** `TestRed_RT02_DeadWANDoesNotAttractTraffic` passes
(`good=190 dead=10/200` typical runs; <=40 allowed) and was removed from
`DEFAULT_ALLOW` — `scripts/red_gate.sh` exit 0 with **3 entries** (RT-01,
RT-10, RT-11); full red suite **3 FAIL / 8 PASS / 0 SKIP**. `red_test.go`
untouched: assertion-line grep diff vs HEAD is empty.

**TDD:** T-HLT-01 window math + both ejection thresholds; T-HLT-02 backoff
doubling to the 10min cap + `ejectCount` decay after 10min of health;
T-HLT-03 all-canaries-fail => `env_degraded`, empty failing list, nobody
ejected (75% boundary both ways); T-HLT-04 half-open: backoff gate, 1 vs 2
consecutive canary successes, streak reset; T-HLT-05 `Suspect -> healthy`
recovery (OK relays alone never re-admit; backoff + 2 canaries do; fresh
streak re-ejects); T-HLT-06 one dead TARGET never ejects a healthy path
(5 consecutive failures to one host stay put; 3 hosts eject). All use an
injected fake clock — no sleeps. Root `canary_test.go` additionally pins the
knob mapping and the production wiring of T-HLT-03 (2/2 failing paths =>
nothing recorded, nothing drained, gauge set).

**Mutation sanity (evidence):** with `Path.RecordOutcome` reverted to the
pre-F-01 behavior (`RecordSuccess` unconditional, window not fed),
`TestRed_RT02_DeadWANDoesNotAttractTraffic` fails exactly like the
baseline: `good=26 dead=174 dead.ConsecutiveFails=0` — "dead WAN received
174/200 connections". Restoring the rule turns it green again
(`good=190 dead=10`), `go vet ./...` clean.

**Deviation (flag for the metrics/observability task):** SPEC-H.6 asks for
gauge `viberoxy_env_degraded`; `metrics*.go` is owned by a parallel task, so
the gauge lives in `internal/health` (`SetEnvDegraded`/`EnvDegraded` atomic)
plus a `slog.Warn("env_degraded: ...")` line. Expose it as
`viberoxy_env_degraded` when metrics.go lands. The `HEALTH_ENDPOINTS` env
var is read directly by `internal/health` (same dual-read pattern as
`subs.AllowHTTPFromEnv`) — `proxycfg.ParseConfig` does not validate it yet
(flag for a later config task).

**Pitfall (host, not code):** back-to-back `go test -race` suites on this
macOS host exhaust the loopback ephemeral port range — unrelated tests in
untouched packages then fail with `dial tcp 127.0.0.1:<port>: connect:
can't assign requested address` (TIME_WAIT churn; `netstat` is silent in
the sandbox, so it looks like a code failure). Wait ~60s between heavy
runs; a cooldown re-run of `go test -race -shuffle=on -count=1 ./...` is
fully green (rc=0, 13/13 packages).
## task-3-3: make-before-break `DropAndReplace` on spare ports (F-13, RT-10 green)

One commit on `task-3-3-swap`: `fix(rotation): make-before-break DropAndReplace on spare ports (F-13); RT-10 green`.

New sequence in `wan.go` (`DropAndReplace`), all failure paths leave the slot exactly as it was:

1. **pick** under `replaceMu` (snapshot occupant + `Candidates.Best(exclude)` with the
   F-12 HasServerPort rule) — the lock is then released;
2. **test** the candidate on a port Acquired from `opts.TestPorts` (released the moment the
   test returns; exhaustion => `ErrNoReplacementCandidate`-family error, never a reused port)
   while the OLD occupant stays Active and serving — this is RT-10's assertion;
3. **start** the replacement on a spare service port (`opts.SparePorts`; an empty slot keeps
   its own free port), so the old process never loses its listener;
4. **`WANPool.Swap(index, expected, newPath, configPath, releasePort)`** — the atomic cutover
   under `replaceMu` + `slot.mu`: config/cmd/config-path/service-port move to the replacement
   (front-end dials follow `slot.ServicePort`), the retired Path goes `path.Draining`, probe/
   speed bookkeeping clears. It refuses (`ErrNoReplacementCandidate`-wrapped, nothing changed)
   when the slot no longer holds `expected` — another replacement won, or a reset happened
   mid-test; the loser stops the process it started;
5. **post-swap teardown**: stop the retired process, THEN Release its spare (listener down
   before the port can be re-issued). `ResetEmpty` does the same and restores
   `ServicePort = BasePort+index` when the slot held a spare.

`replaceMu` is now held ONLY around the pick and the cutover — never across the speed test or
the process start, so concurrent drops of different slots test in parallel.

Wiring (`startup`): `ports.NewTestPorts(TestBasePort, MaxTestPerCycle+WanCount)` and
`ports.NewSpareWANPorts(WanBasePort, WanCount, 2*WanCount)` (2x because an occupied slot still
holds its spare while the replacement checks out the next — one full round of concurrent swaps
needs two per slot; invalid ranges log+fall back to legacy behavior rather than refusing to
boot), stored via `pool.SetPortAllocators`, threaded into `DropAndReplaceOptions` by
`NewAPIHandler`, and drawn from by `runCycle`/`startup` through `acquireCycleTestPort`
(fallback: historical `TEST_BASE_PORT+seq` when a hand-built pool has no allocator).

Tests: `swap_test.go` — T-SWAP-01 (full swap under ~200 rps load through the SOCKS front-end,
zero failed connections; FakeWAN/FakeClient harness), T-SWAP-02 x3 (drop tests draw from the
shared allocator with exhaustion+release accounting; runCycle draws from the same allocator;
concurrent drops never share a test port and do overlap), T-SWAP-03 (already-serving
server:port skipped at the pick + cands Best cross-check), failure path (failed test => old
WAN still serving through the front). `api_test.go` flipped three break-before-make assertions
to the new contract (no-candidates, test-failure, start-failure now all leave the slot Active
with the old WAN — renamed `..._StartFailureKeepsOldWAN`). `red_test.go` untouched (assertion
grep diff vs HEAD empty — plumbing needed no changes at all).
`scripts/red_gate.sh`: RT-10 removed from `DEFAULT_ALLOW` — allow list 3 entries, gate exit 0;
red suite 3 FAIL (RT-01/02/11) / 8 PASS / 0 SKIP. Mutation sanity: re-inserting the old
pre-test `ResetEmpty` fails `TestRed_RT10_...`; removing it turns RT-10 green again.

**Flags for later tasks:**

- `relay.go:dialWAN` still reads `Slots[wanPath.Slot].ServicePort` WITHOUT the slot lock
  ("fixed for the slot's lifetime" is no longer true after a swap). `probeAllWANs` was moved
  onto the lock by this task; the relay read is the known remaining spot — the drain/relay
  task should dial the held path's immutable `wanPath.Port` instead, which also removes the
  held-path-vs-slot disagreement. T-SWAP-01 quiesces its workers around the cutover (documented
  in the test) so `-race` stays clean until then; in-flight conns across the retired process's
  teardown are the later drain policy's job (DRAIN_MAX / inflight==0).
- Spare sizing (2xWanCount) and the TestBasePort/Spare range overlap check live in `startup`;
  a config task that adds env knobs should validate `TEST_BASE_PORT..+MAX_TEST_PER_CYCLE` not
  colliding with `WAN_BASE_PORT+WAN_COUNT..`.

## task-2-7: draining semantics — selection exclusion, inflight/DRAIN_MAX completion, un-drain (F-02, D-03)

One commit on `task-2-7-drain`: `fix(wan): draining excludes selection, inflight-based drain
completion, un-drain (F-02); RT-01 green`.

The F-02 contract is now: **draining = no new connections, existing flows finish, then it
stops** (or DRAIN_MAX hard-kills it); a health drain may reverse, a replacement drain may not.

- **Selection (`wan.go`)**: `GetLeastLoadedExcluding`'s routable pass selects `StateActive`
  only — `StateDraining` is skipped exactly like a tried/over-threshold path. The degraded
  fallback walks ACTIVE slots first (byte-for-byte body of the old loop) and only when no
  active path is available considers draining slots, in a new third pass with its own
  `slog.Warn("no active WAN: draining slot selected as last resort")`; the existing
  "no routable WAN" WARN stays where it was. Everything else in the rule (least-loaded, ties
  to lowest index, Suspect skip, tried exclusion) is untouched.
- **`RoutableCount` counts ACTIVE routable slots only (D-03)**: `/readyz` is ready iff >= 1
  active routable path; a draining-only pool is 503.
- **Drain completion (`DrainExpired(maxDrain)`)**: a draining slot completes when its current
  path's `Conns() == 0` (flows finished) OR `now-DrainAt >= maxDrain`, whichever first —
  replacing the fixed `max(60s, 2×FETCH_INTERVAL)` timer. `runCycle`'s reap step keeps its
  shape and now receives **DRAIN_MAX** (`DefaultDrainMax = 600s`, env `DRAIN_MAX` in seconds,
  invalid/absent → default with a warning, read by `drainMaxFromEnv()` in `main.go`).
- **Drain reason + un-drain**: `WANSlot.drainReason` (`DrainReplace`/`DrainHealth`, zero value
  = replace so hand-built draining fixtures are never un-drained). `MarkDraining` keeps its
  signature and now means the REPLACEMENT flavor (runCycle's frozen replacement block calls
  it); the canary's marking switched to the new `MarkDrainingHealth`. `UnDrainIfRecovered(idx,
  halfOpenRecovery)` returns a health-drained slot to Active when `Path.NoteCanary` reported
  half-open re-admission or the consecutive canary-success streak reaches
  `health.HalfOpenSuccesses`; it is called from `runCanaries` (fast return) and from
  `DrainExpired` (reap-time guard: recovery beats the kill). Replacement drains and Swap's
  retired generation are never un-drained.
- **`internal/health` (small hook)**: `State.NoteCanary` now increments `canaryOK` on a
  success for a NON-suspect path too (it already reset it on failure/ejection, and half-open
  counting is unchanged), and `State.CanaryStreak()` exposes it; `Path.CanaryStreak()`
  (nil-safe) forwards it. That was the missing "recovery streak" the un-drain hooks.

**Tests:** new `drain_test.go` — T-DRAIN-01 (real SOCKS front + testutil FakeWAN: a paced
2s transfer through a slot that goes Draining mid-flight completes with zero resets while 12
new connections all land on the active WAN, and `DrainExpired` refuses to reap it until
inflight hits 0), T-DRAIN-02 (`DefaultDrainMax == 600s`; inflight==0 completes, 601s age
hard-kills inflight>0, 100s age with flows stays), T-DRAIN-03 (`runCanaries` against a local
endpoint through a real SOCKS relay: streak 1 stays draining, streak 2 un-drains to Active
and selectable; the replacement-marked slot survives the same recovery and is reaped
normally; reap-time guard un-drains a pre-recovered health drain instead of killing it),
selection property (GetLeastLoaded + GetLeastLoadedExcluding tried variants + degraded-active
beats healthy-draining + draining-only last resort + RoutableCount D-03 both ways) and
`drainMaxFromEnv`. `wan_test.go`'s `TestDrainExpired`/`TestDrainExpired_NotYet` gained an
in-flight reservation each so the age rule still decides them (assertion lines unchanged).

**Red suite / gate:** `TestRed_RT01_DrainingSlotNotSelected` passes and was removed from
`DEFAULT_ALLOW` — `scripts/red_gate.sh` exit 0 with **1 entry (RT-11, owned by a parallel
task)**; full red suite **1 FAIL (RT-11) / 10 PASS / 0 SKIP**. `red_test.go` untouched
(assertion grep diff vs HEAD empty).

**Mutation sanity (evidence):** (1) re-including `StateDraining` in the routable pass fails
`TestRed_RT01_DrainingSlotNotSelected` ("GetLeastLoaded picked draining slot 0; want active
slot 1") and the selection property (5 assertion lines incl. every `GetLeastLoadedExcluding`
case); restoring turns both green. (2) killing the `inflight == 0` completion (pure timer)
fails all three T-DRAIN tests ("DrainExpired(600s) = [1], want [1 2]" etc.); restoring turns
them green. `gofmt -l .` empty, `go vet ./...` clean, `go test -race -shuffle=on -count=1
./...` green (13/13).

**Stale AGENTS.md/README pointers for whoever consolidates:** the WAN state machine line
("draining → (kill after max(60, 2×FETCH_INTERVAL))" → now "inflight==0 or DRAIN_MAX, 600s
default"), "Routable WANs" condition 1 ("StateActive or StateDraining" → active only for
selection/readiness; draining only as last-resort fallback), the "Load-balancer fallback"
paragraph, `RoutableCount`'s Key-Functions row, `runCycle(cfg, pool, candidates, grace)` →
`drainMax`, and the README env table needs `DRAIN_MAX` (seconds, default 600).
## task-3-4: continuous evaluation + hysteresis rotation (F-03, SPEC-R)

One commit on `task-3-4-rotation`: `fix(rotation): continuous evaluation + hysteresis swaps (F-03/SPEC-R); RT-11 green`.

**F-03 — full pool keeps evaluating.** `runCycle`'s loop head no longer breaks when the pool
is full: it recomputes `poolFull` per config and, while full, tests up to
`maxTestPerCycleFull = 2` NEW candidates per cycle (fill mode keeps `MaxTestPerCycle`).
"New" = `HasServerPort` misses (active/draining) AND the Raw is not in `recentlyTested` —
seeded from `candidatePool.List()` (the cands TTL ages the memory) plus every config tested
this same cycle. Results feed the candidate pool exactly as before, so the old replacement
block's `len(results) > 0` precondition is finally satisfiable.

**SPEC-R — at most one make-before-break swap per cycle.** New `rotator` in `main.go`
(`newRotator()`; process-wide `rotation`): knobs `rotationHysteresis = 0.30`,
`rotationMinDwell = 10m`, injected `now`/`exitIP`/`test`/`start`, `lastSwap` cooldown
reference. `decide()` gate order: FETCH_INTERVAL cooldown → worst incumbent (lowest score;
`PickReplacementSlot` instability preference breaks ties) → MIN_DWELL via the incumbent
path's `CreatedAt` (minted at activation) → per-candidate gates (speed bar, HasServerPort,
exit-IP dedupe through `bestNewCandidate`'s exclude callback) → hysteresis
`candidate >= (1+0.30) x incumbent`. `maybeSwap()` runs exactly one
`DropAndReplace` with a single-entry cands pool (decision and swap cannot disagree; the
mandatory re-test + spare-port start stay intact) and stamps `lastSwap` only on success.
The old `MarkDraining` replacement block (unreachable dead code) is gone;
`MarkDraining` itself is untouched (canary still owns it).

**Simplifications / flags for later tasks:**
- Score = benchmark `Speed` for now; the live goodput/TTFB EWMA score lands with the
  scheduler task (`path.RecordHealth` EWMAs are still unused by rotation).
- Candidate exit IP: production resolver `candidateExitIP` = the config's server address
  when it is an IP literal, else unknown → gate skipped; the served side reads the
  `ExitIP` the canary loop records per slot. When the scheduler task can carry observed
  exit IPs onto candidates, swap the resolver.
- Knobs are code constants — no env plumbing; a config task should add
  `MAX_TEST_PER_CYCLE_FULL` / `HYSTERESIS` / `MIN_DWELL` env parsing in `proxycfg.ParseConfig`.

**TDD / evidence:** `rotation_test.go` — T-ROT-01 (equal/+10%/+29% candidate ⇒ no swap),
T-ROT-02 (>=30% better ⇒ exactly one swap, cooldown blocks the next), T-ROT-03 (MIN_DWELL
+ cooldown, injected clock), T-ROT-04 (exit-IP duplicate rejected; different/unknown pass),
T-ROT-05 (full-pool budget 1..2 evaluations/cycle). `TestRed_RT11_FullPoolStillEvaluatesCandidates`
green with `red_test.go` completely untouched (assertion grep diff vs HEAD empty: 22
`t.Fatal*/t.Error*` lines both sides — sorted.txt already reflects the new evaluator).
Mutation sanity: deleting the hysteresis condition makes T-ROT-01 fail
(`decide() = victim 0 candidate &{... Speed:11.9 ...} (swap)`); restoring it goes green.

**red_gate.sh:** RT-11 removed from `DEFAULT_ALLOW` (1 entry left: RT-01, a parallel
task's). Empty allow list now has success semantics — "every TestRed_ must pass; zero
failures allowed" — instead of `exit 1` on emptiness; bash-3.2 `set -u` empty-array
expansions guarded with `${arr[@]+...}`. On this branch `RED_GATE_ALLOW='' bash
scripts/red_gate.sh` prints the empty-list note then fails with
`unexpected failure: TestRed_RT01_DrainingSlotNotSelected` (exit 1), proving the empty
path enforces all-pass rather than erroring on emptiness; default run exits 0 with
1 entry (failing=1 passing=10 skipping=0).

## task-245: P2C scheduling + (client, site) affinity (F-15, F-05)

Two commits on `task-245-sched-affinity`:
1. `feat(sched): P2C with peak-EWMA cost and atomic reservation (F-15)`
2. `feat(affinity): HRW per (client, site) with bulk spill (F-05)`

**Selection now lives in `internal/sched` (production path).** `WANPool.Select(SelectOptions)`
is the entry point: `eligibleTries(tried, threshold, strict)` builds the three
eligibility tiers ONCE (shared with the legacy `GetLeastLoadedExcluding`), then
`sched.Select(Request)` ranks and reserves. `dialWANFailover.next()` in relay.go is the
only production caller (front-ends pass `clientIdentity(...) + targetHost` through it).

- **Cost** = `(Inflight+1) * max(ewmaTTFB, 50ms floor) / weight`; `weight =
  clamp(goodputEWMA/medianGoodput, 0.25, 4) * ramp(age) * stateFactor` (Active 1.0,
  Suspect 1.5, Draining 2.0, Probation/Dead 4.0 — penalize, never exclude, inside a
  tier). Tier with no goodput signal (median 0) uses neutral weight 1.0. Zeros are
  included in the median.
- **P2C**: two distinct candidates sampled (`j=(i+1+Intn(n-1))%n`), cheaper cost wins;
  ties are EXACT cost equality -> seeded coin (never slot index). No near-tie
  tolerance: equal inputs are bitwise-equal, and the ramp's clamp to 1.0 past
  `RampWindow` (60s) makes equal-age-class generations exactly tie, while a
  sub-clamp age gradient is the slow-start's intended preference (this is also what
  keeps the pre-existing fixtures whose comment says "tie -> lowest index" — they
  activate slots in index order, so oldest == lowest index there — deterministic).
- **Atomic select+reserve**: rank + `Inflight.Add(1)` + release closure happen inside
  one package-wide `selMu` critical section; `SeedRand` reseeds under the same lock
  (tests). `Select` returns `(nil, nil)` iff every tier is empty (503 / REP 0x01 kept).
- **Slow-start ramp** `ramp(age)=min(1, 0.1+0.9*age/60s)` off `Path.CreatedAt`.
- **peak-EWMA (`path.RecordHealth`)**: worse sample jumps immediately (TTFB up,
  goodput down), better sample decays with tau 10s/30s via
  `1-exp(-dt/tau)` between samples (new `healthAt` timestamp, injected-clock
  `recordHealthAt` for tests). The old fixed `ewmaAlpha=0.25` smoothing is gone.
- **relay.go feeds (stats only, `logAccess` untouched)**: goodput = OnBytes down
  total over relay duration; TTFB = time-to-first-byte at the relay, with the
  documented SIMPLIFICATION that a relay with no observable first down byte falls
  back to the relay DURATION (coarse upper bound) instead of skipping the sample;
  still fed only on `outcome == OK` (F-01 unchanged).

**Affinity (`internal/affinity`, F-05).** `Pick(clientID, siteKey, cands)` = FNV-1a-64
over `clientID NUL siteKey NUL LE(path.ID)`, max score wins (score tie -> first
candidate, deterministic). `SiteKey(host)` strips port/brackets/root-dot, keys IP
literals on `net.IP.String()`, hostnames on an embedded eTLD+1 heuristic (last two
labels; last three under the embedded second-level exception list — co.uk, com.au,
co.jp, com.br, co.kr, com.tr, com.cn, co.za + ~20 more pairs; NOT the full PSL —
`x.github.io` reduces one label early, documented as the maintenance knob).
`NO_AFFINITY_DOMAINS` is comma/whitespace-separated, case-insensitive, exact or
parent-domain suffix match, read at REQUEST time (dual-read like
`subs.AllowHTTPFromEnv`; startup validation in `proxycfg.ParseConfig` is a flag for a
config task — proxycfg is outside this task's scope).
`clientIdentity(remoteAddr, authUser)` (relay.go) = authenticated username when
PROXY_USERS identified the connection (SOCKS5 subnegotiation now RETURNS the
username; CONNECT captures it from Proxy-Authorization), else client IP with the
ephemeral port stripped. **NAT caveat:** without PROXY_USERS, clients behind one NAT
share one affinity bucket (per-SITE stickiness); usernames restore per-user keys.

**AFFINITY_BULK_SPILL**: HRW pick accepted while `pick.Conns() <= 3 x medianConns(tier)`
(median includes the pick; even-length averages the two middle values), otherwise
fall through to P2C. Literal zero-median consequence (documented): with >=3 paths
where the others are idle (median 0) ANY inflight on the sticky path spills — so
parallel same-site opens spread across paths while sequential requests stick;
two-path pools never spill (median = k/2 >= k/3 always). Availability over
stickiness, as specified.

**Retry loop (`relay.go dialWANFailover`)**: `next()` = `pool.Select` with the tried
set; the path arrives ALREADY reserved, so the per-attempt
`metricProxyConnections.Inc` moved out of `beginWAN` (beginWAN itself is unchanged
and still reserves — red RT-03 / T-PATH-01 call it directly). A `pendingRelease`
tracks the last handed-out reservation: consumed by that attempt's dial (failure ->
release, success -> caller's `endWAN`), and released after `retry.Run` if the retry
bucket made Run drop a candidate it never dialed — no reservation can leak.
Eligibility of `GetLeastLoadedExcluding` and `Select` is byte-identical except the
routable tier's path-state rule (Select requires `state == Active`, which excludes
Probation/Draining/Dead generations an Active slot never holds in production —
fixtures only; the legacy call keeps `!= Suspect`).

**Legacy**: `GetLeastLoaded`/`GetLeastLoadedExcluding` keep the least-loaded ranking
and are now TEST-ONLY (wan_test.go's TestGetLeastLoaded*, red RT-01, drain selection
property all pin them; no production caller remains). Deliberately NOT reimplemented
on top of P2C: their tie rule (lowest index) would flake.

**TDD / evidence.** Red first against a naive tier[0]-only stub: T-SEL-02/03/04/06 all
failed (`path 0 received 100000 of 100000`, `total inflight = 0`, `path 0 won 1000 of
1000`); green after the real implementation. T-AFF-01/03/04 red before the affinity
hook landed (`stickiness = 0.2424`, `fallback pick 1 = slot 3, want slot 2`, `sticky at
exactly 3x median spilled to slot 1`), green after. Final suites —
`internal/sched`: T-SEL-01 (root, 1e5 seeded picks: only healthy Active paths while a
routable tier exists; degraded members incl. Suspect/Probation/vacant only after the
healthy paths are tried; draining only as last resort), T-SEL-02 fair split
(25000+-15% over 1e5 held picks), T-SEL-03 2x goodput -> ~2x load (ratio bounds
1.6..2.5), T-SEL-04 (256 goroutines x 1000: exact 256000 inflight, per-pick
visibility, drain to 0), T-SEL-05 ramp formula + fresh path <=5% of 1000 picks,
T-SEL-06 ties 400..600; T-AFF-01 (600 sequential ticks, SLO >= 99.5%, 100% sticky, 10
simulated ejection ticks fall back), T-AFF-02 (removing 1 of 8 moves exactly the
victim's keys, <= 1/8 + 3% of 10000), T-AFF-03 deterministic next-HRW fallback,
T-AFF-04 spill boundary (3x median sticks, 10>3 spills); affinity package unit tests
(SiteKey matrix, NO_AFFINITY_DOMAINS, HRW determinism/distribution);
root wiring tests (`clientIdentity`, `socksAuthenticate` returns the username, SOCKS
e2e: two sequential same-key connections land on the same slot).
**Mutation sanity:** (1) moving `chosen.Reserve()` out of `Select` (select+reserve
split) -> T-SEL-04 FAIL (`total inflight after 256000 selects = 0`, then negative
counts from the unbalanced releases); restore -> green. (2) tie-break -> lowest slot
index -> T-SEL-06 FAIL (`path 0 won 1000 of 1000`); restore -> green.
`gofmt -l .` empty, `go vet ./...` clean, `go test -race -shuffle=on -count=1 ./...`
green (15/15), `bash scripts/red_gate.sh` exit 0 (failing=0 passing=11 skipping=0,
empty allow list).

**Existing-test touch (flag):** `drain_test.go` `TestDrain_TDRAIN01` pins WHICH slot
receives the first transfer (its fixture comment relies on "tie -> lowest index").
Affinity (F-05) makes the first pick HRW-determined, so that one test now sets
`t.Setenv("NO_AFFINITY_DOMAINS", "1.2.3.4")` for its synthetic target — NO assertion
line changed; selection falls back to ramp-biased P2C whose "oldest == slot 0"
premise the comment already documents. No other existing test needed changes.

**Stale AGENTS.md/README pointers for whoever consolidates:** the
`GetLeastLoaded(thresholds...)` Key-Functions row (production selection is
`WANPool.Select` -> `internal/sched` P2C + HRW affinity; GetLeastLoaded* is legacy
test-support), the "Load-balancer fallback" paragraph (ranking is P2C/peak-EWMA cost,
ties random, slow-start ramp; the fallback ORDER — routable -> degraded -> draining —
is unchanged), the directory map needs `internal/sched/` + `internal/affinity/`, and
the README env table needs `NO_AFFINITY_DOMAINS` (comma/suffix list, default empty).
# AGENTS.md consolidation draft (apply when approved)

> **Constraint:** AGENTS.md is approval-blocked for subagents — writing it
> requires interactive user approval. This block is the complete replacement
> text; apply it verbatim to AGENTS.md the moment approval is granted. It
> absorbs every stale-pointer note accumulated above (package map, key
> functions, data flow, env table at parity with README.md).

## Entry Points

| If you need to... | Start here |
|---|---|
| Change env vars / startup validation | `internal/proxycfg/config.go` — `ParseConfig()` (all knobs, hard-exit on bad values); `main.go:main()` only calls it |
| Add a new proxy protocol parser | `internal/proxycfg/parse.go` — `ParseSingleErr` switch (+ `IsXraySupported` leak guard; hysteria2/tuic/wireguard must never be promoted) |
| Change split-routing rules | `internal/proxycfg/router.go` — `Router.Decide`, suffix lists (host-string match) |
| Change how speed tests work | `tester.go` — `TestSpeedWithStability`, `DownloadMeasurer`; mux parity via `xray.go:xrayMuxForRun` |
| Change xray config generation | `internal/xraycfg/build.go` — `BuildXrayConfig`, `buildOutbound` (`xray.go` is a thin wrapper + type alias) |
| Health-check / stop a running xray | `internal/xrayproc/proc.go` — `Handle.Alive/Stop/Exited` (Wait-based liveness); wrappers `HealthCheckXray`/`StopXray` in `xray.go` |
| Change WAN lifecycle / selection / drain | `wan.go` — slot state machine, `GetLeastLoadedExcluding`, `DrainExpired`, `UnDrainIfRecovered`, `DropAndReplace`/`Swap` |
| Change path identity / inflight accounting | `internal/path/path.go` — one `Path` per xray lifetime (F-04 root fix) |
| Change passive health / canaries | `internal/health/` — `Classify`/`ClassifyReason`, `State` window/eject/backoff, `ApplyCanary`; wiring in `main.go:runCanaries` |
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
├── wan.go               — WAN pool: slot state machine, selection (3 passes), drain/un-drain, Swap/DropAndReplace
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
│   ├── retry/           — bounded dial-stage failover (T-RETRY safety: dial stage only)
│   ├── cands/           — candidate pool (dedupe by Raw, TTL, per-server cap)
│   ├── auth/            — bind policy, PROXY_USERS/API_TOKEN, SSRF guard, redaction
│   ├── subs/            — subscription fetch (8 MiB cap, https policy, ETag/Last-Modified)
│   ├── relayio/         — bidirectional splice (idle/handshake timeouts, half-close, peek reader)
│   ├── xraycfg/         — xray JSON config generation + golden tests
│   ├── xrayproc/        — xray process lifecycle (Wait-based liveness, zombie reaping)
│   ├── ports/           — speed-test + spare-service port allocators (F-13)
│   └── testutil/        — FakeWAN harness for integration tests
├── scripts/red_gate.sh  — CI gate for the red acceptance suite (allow list now EMPTY)
├── README.md            — user docs: env table, breaking changes, limitations (source of truth for knobs)
├── AGENTS.md            — this file (writes require user approval; drafts live in agents_temp.md)
├── LICENSE
└── go.mod                — module viberoxy, go 1.21.0, zero dependencies
```

Deleted/renamed files the old AGENTS.md still referenced: `parser.go`
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
  → wan:  dialWANFailover — GetLeastLoadedExcluding(tried) + retry.Run (≤ 3 attempts,
    shared 5 s budget, token bucket); 503 = no candidate, 502 = all dials failed
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

A path is selectable only when it is **Active**, under `WAN_FAIL_THRESHOLD`,
has a live process, is not `Suspect`, and is not already tried on this
connection (`GetLeastLoadedExcluding`, least-loaded, ties → lowest index).

1. Routable pass (above) — never includes draining.
2. Degraded fallback: least-loaded among **active** slots even over threshold
   (logged, counted by `viberoxy_selection_last_resort_total`).
3. Last resort: no active path at all → a **draining** slot may take the
   connection (also logged + counted).

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
| `GetLeastLoadedExcluding(tried, thresholds...)` | `wan.go` | Three-pass selection, returns `*path.Path` (nil = none) |
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
| `DRAIN_MAX` | `600` | Seconds; invalid/unset → default + warning (`drainMaxFromEnv`) |
| `HEALTH_ENDPOINTS` | gstatic `generate_204` + cloudflare `cdn-cgi/trace` | Canary endpoints; ipify never used for health |

Code-only knobs (no env yet): `maxTestPerCycleFull = 2`,
`rotationHysteresis = 0.30`, `rotationMinDwell = 10m`, retry policy
(3 attempts / 5 s / 10 % ratio / burst 10), all SPEC-H thresholds (window,
ejection rules, backoff, 75 % env breaker). There is **no**
`NO_AFFINITY_DOMAINS` (or any affinity knob): affinity is not implemented.

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

## Superseded pointers (what this draft replaces)

Every stale pointer the campaign notes recorded, and where it now lives:

| Old (stale) | Now |
|---|---|
| "Change env vars → `main.go` — `parseConfig`" | `internal/proxycfg/config.go` — `ParseConfig` |
| "Add parser → `parser.go` — `ParseSingle` switch" | `internal/proxycfg/parse.go` — `ParseSingleErr` |
| Directory rows for `parser.go`, `router.go`, `candidate.go` | `internal/proxycfg/parse.go`, `internal/proxycfg/router.go`, `internal/cands/pool.go` |
| "Change xray config generation → `xray.go` — `BuildXrayConfig`, `buildOutbound`" | `internal/xraycfg/build.go` (`xray.go` wraps) |
| "Health-check / stop xray → `xray.go`" | `internal/xrayproc/proc.go` |
| Key Functions rows `parseConfig()`, `ParseConfigs()`, `ParseSingle()`, `BuildXrayConfig()`, `StartXray()`, `StopXray()` | see Key Functions table above |
| `WANSlot.ConnCount` / `WANSlot.ConsecutiveFails` / `IncConnCount` / `DecConnCount` | `internal/path` — `Path.Inflight`, `Reserve`/`Release`, `ConsecutiveFails` |
| `GetLeastLoaded(...) int` (slot index) | `GetLeastLoaded/GetLeastLoadedExcluding(...) *path.Path` |
| "empty → … → draining → (kill after max(60, 2×FETCH_INTERVAL))" | drain completes at `inflight == 0` or `DRAIN_MAX` (600 s); health drains may un-drain |
| "Routable: state Active **or Draining**" | selection/readiness: ACTIVE only; draining only as the counted last resort |
| "Load-balancer fallback … active/draining" | three passes: routable active → degraded active → draining last resort |
| "HealthCheckAll runs every `KEEPALIVE_INTERVAL`" | runs each `runCycle`; `KEEPALIVE_INTERVAL` now drives the 15–30 s canary band |
| `runCycle(cfg, pool, candidates, grace)` | `runCycle(cfg, pool, candidatePool, drainMax)` |
| Data flow "Replace one low-performing WAN (drain old → spawn new on same port)" | ≤1 make-before-break swap per cycle on spare ports, gated by cooldown/dwell/hysteresis |
| Data flow mentioning `TestAll` (F-21) | gone; `TestSpeedWithStability` + `runCycle` |
| `viber-console` supervisor reference (F-21) | not in this repo — drop |
| README env rows: `XRAY_MUX` default `true`, "SOCKS no auth", missing `DRAIN_MAX`/`LISTEN_ADDR`/`ALLOW_*`/`API_TOKEN`/`HEALTH_ENDPOINTS`/`ALLOW_HTTP_SUBSCRIPTION` | README.md rewritten; env table above at parity |
| Red-gate allow list entries (RT-01…RT-11) | empty — all 11 red tests pass |

## task-4-3: chaos + integrity verification suites (review §7 L2–L3)

New files (all `package main`; nothing in `internal/` or production code changed):

| File | Contents |
|---|---|
| `integrity_test.go` | T-INT-01 cross-talk-under-chaos (12 clients: 8 SOCKS5 `testutil.FakeClient` + 4 HTTPS-CONNECT `httpFakeClient`, 200 conns/iteration, seeded chaos: Die/Refuse/AcceptClose/SlowTTFB/Flaky/RstAfter flips + repairs, health-drain, forced SPEC-H.3 ejection, one quiesced make-before-break swap with a 96 KiB paced transfer held across it) and T-INT-02 (256 MiB SHA-256 stream survives a mid-stream swap of its own slot; 32 MiB under `-short`). Shared helpers: `startSocksFront` (explicit `WanFailThreshold`), `startIntegrityProxy`, `httpFakeClient`, `loadWorkers`, `integrityCollector`/`classifyExchange`, `streamTransfer`/`transferExpectedHash`, `coverServicePortReads`. |
| `chaos_test.go` | `//go:build chaos`; T-CHAOS-01..09 with asserted budgets (2s selection exit, 20s window ejection, traffic-share drops, env_degraded, zero churn, ≤1 re-admit/60s via injected clocks, swap-under-load zero failures, mid-stream swap hash). |
| `chaosbuild_test.go` / `chaosbuild_chaos_test.go` | the `chaosBuildTag` const pair (`!chaos` / `chaos`) — T-INT-01's iteration profile switch (2 normal, 25 behind the tag, `CHAOS_ITERATIONS` overrides both). |
| `.github/workflows/ci.yml` | new `chaos` job: `go test -tags chaos -run 'TestChaos|TestIntegrity' -timeout 15m ./...`; `test` job untouched (it picks up the 2-iteration T-INT-01 + T-INT-02). |

Layout decisions worth knowing:

- `-race` swap ordering: `relay.go` reads `slot.ServicePort` without the slot lock
  (by design, per swap_test.go), so a swap may only write it once every reader is
  ordered before the write. Instead of swap_test's drain-to-zero (which cannot hold
  while a transfer is in flight), T-INT-01 pauses new sends, waits 300 ms, then
  acquires the metric mutexes handlers touch AFTER that read (byte callbacks,
  retry/outcome events, latency sample) — one acquire of each orders all those
  reads — and acquires `metricProxyConnections` after the write, before resuming,
  ordering every future read. Validated green under `-race`.
- FakeWAN `Blackhole` leaves handlers wedged until the idle timeout (outcome
  recorded only at relay end), so `AcceptClose` is the black-hole flavor used in
  chaos tests (probe: it classifies HardFail/`no_down` immediately); the front's
  `IdleTimeout` is injected (6–60s) wherever a wedged backend could pin
  reservations past a budget.
- T-CHAOS-02/04 run the front with `WanFailThreshold = health.WindowSize` so the
  consecutive-failure counter cannot remove a path before the SPEC-H.3 window
  accumulates its evidence — the tests pin the window ejection itself — and use
  `driveBurstsUntil` (relaunch a burst while the condition is unmet): a victim
  whose scheduler weight has diverged earns too few picks for its window to fill
  inside one fixed burst, and a starved wait is how the suite first went red.
- T-CHAOS-07 drives `Path.RecordOutcome`/`NoteCanary` with an injected clock
  (runCanaries has no clock knob); a successful re-admit also calls
  `RecordSuccess()`, mirroring `runCanaries`.
- Mutation sanity (reverted): making `endWAN` release the slot's CURRENT path is
  caught by T-INT-01's per-generation balance assertion — retired generation
  leaks `inflight=1`, replacement goes to `inflight=-1` on every iteration (no
  cross-talk bytes: `Conns()` clamps at zero, so accounting — not routing — is
  what this mutation breaks).
