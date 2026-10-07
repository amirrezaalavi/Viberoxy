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
