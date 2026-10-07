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
