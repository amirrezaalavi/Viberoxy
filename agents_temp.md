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
