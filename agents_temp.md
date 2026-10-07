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
