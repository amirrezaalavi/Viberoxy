# Implementation Steps

## Step 1: Project skeleton + env var validation

**Files:** `main.go`

Things to do:
- Declare `package main`
- Parse all env vars with `os.Getenv`, validate each against rules in PLAN.md
- Use a `Config` struct to hold parsed values
- On invalid value: `fmt.Fprintf(os.Stderr, "...")` + `os.Exit(1)`
- On missing required var: same
- `main()` calls `parseConfig()` then calls `startup(cfg)`, then enters the main loop
- Handle SIGINT/SIGTERM for graceful shutdown

**Tests:** `main_test.go`
- `TestParseConfig_Defaults` — no env vars set, verify defaults match PLAN.md
- `TestParseConfig_ValidOverrides` — set a few vars, verify parsed correctly
- `TestParseConfig_InvalidTimeout` — `TEST_TIMEOUT=1` → exit 1
- `TestParseConfig_InvalidWanCount` — `WAN_COUNT=10` → exit 1
- `TestParseConfig_MissingSubUrl` — no `SUBSCRIBER_URL` → exit 1
- `TestParseConfig_BadSubUrl` — `SUBSCRIBER_URL=ftp://bad` → exit 1
- `TestParseConfig_ZeroDownloadSize` — `DOWNLOAD_SIZE=0` → exit 1
- `TestParseConfig_NegativeMinimumSpeed` — `MINIMUM_SPEED=-1` → exit 1

---

## Step 2: Subscription parser

**Files:** `parser.go`

Things to do:
- Port the parser logic from Viberayd's `pkg/parser` (stdlib only — no external deps)
- Must handle: `ss://`, `vmess://`, `vless://`, `trojan://`, `reality://`, `hysteria2://`, `hy2://`, `tuic://`, `wireguard://`, `socks5://`
- Must handle base64-encoded subscription bodies (decode first, then parse per-line)
- Must handle `#Name` fragments
- Return a slice of `ProxyConfig` structs
- `name`, `server`, `port`, `protocol`, `raw` fields minimum

**Tests:** `parser_test.go`
- `TestParseShadowsocks` — valid `ss://` URI
- `TestParseVMess` — valid `vmess://` base64 JSON
- `TestParseVLess` — valid `vless://` URI
- `TestParseTrojan` — valid `trojan://` URI
- `TestParseHysteria2` — valid `hysteria2://` URI
- `TestParseSocks5` — valid `socks5://` URI
- `TestParseTUIC` — valid `tuic://` URI
- `TestParseWireGuard` — valid `wireguard://` URI
- `TestParseBase64Subscription` — base64-encoded list of URIs
- `TestParseInvalid` — garbage input returns error
- `TestParseEmpty` — empty string returns empty slice

---

## Step 3: Xray process lifecycle

**Files:** `xray.go`

Things to do:
- `startXray(cfg *ProxyConfig, servicePort int, logTag string) (*exec.Cmd, error)`
  - Build the xray JSON config in-memory (socks inbound on `127.0.0.1:servicePort`, outbound to the proxy config)
  - Write to a temp file in `os.TempDir()`
  - `exec.Command("xray", "run", "-c", tempFilePath)`
  - Return the `*exec.Cmd` so the caller can kill/check it
- `stopXray(cmd *exec.Cmd) error`
  - Send SIGTERM, wait, then SIGKILL if still alive after 5s
- `healthCheckXray(cmd *exec.Cmd) bool`
  - Check `cmd.Process` is alive / `cmd.ProcessState` is nil

**Tests:** `xray_test.go`
- `TestBuildXrayConfig` — call the config builder with a sample `ProxyConfig`, verify JSON has correct `inbound` port and `outbound` settings
- `TestXrayConfigFileRoundTrip` — write config, read back, verify port matches
- `TestStopXrayNil` — `stopXray(nil)` does not panic
- Mock process test for health check (spawn `sleep`, check health, kill, check health false)

---

## Step 4: Speed tester

**Files:** `tester.go`

Things to do:
- `testSpeed(cfg *ProxyConfig, testPort int, timeout time.Duration, downloadURL string, downloadSize int64) (float64, error)`
  - Start temp xray on `testPort`
  - Dial via local SOCKS5 (`127.0.0.1:testPort`)
  - Time download with `http.Client` through the SOCKS dialer
  - Measure bytes received / time = Mbps
  - Kill temp xray
  - Return speed in Mbps (float64)
- `TestResult` struct: `Config *ProxyConfig`, `Speed float64`, `Error error`
- `testAll(configs []*ProxyConfig, testPortBase int, timeout time.Duration, downloadURL string, downloadSize int64) []*TestResult`
  - Test configs sequentially
  - Return results sorted by speed descending
- Attempt fallback URL if primary fails

**Tests:** `tester_test.go`
- `TestMeasureSpeed` — mock HTTP server that serves `DOWNLOAD_SIZE` bytes, verify speed is ~expected
- `TestMethodFallback` — primary returns error, fallback works → result ok
- `TestMethodTimeout` — server hangs → returns error after timeout
- `TestMethodBothFail` — both primary and fallback fail → error result
- `TestSortResults` — unsorted results in, verify sorted output

---

## Step 5: WAN slot state machine

**Files:** `wan.go`

Things to do:
- `WANState` enum: `Empty`, `Testing`, `Active`, `Draining`
- `WANSlot` struct:
  - `state WANState`
  - `config *ProxyConfig`
  - `cmd *exec.Cmd` (xray process)
  - `connCount int64` (active proxy connections, atomic)
  - `drainDeadline time.Time`
  - `servicePort int`
  - `mu sync.Mutex`
- `WANPool` struct: slice of `WANSlot`
- Methods:
  - `NewWANPool(count int, basePort int) *WANPool`
  - `StartTesting(slotIdx int, cfg *ProxyConfig, testPort int) error`
  - `PromoteToActive(slotIdx int) error` — kill test xray, start service xray on service port
  - `MarkDraining(slotIdx int)` — set state, record drain deadline
  - `KillSlot(slotIdx int)` — stop xray, reset to empty
  - `HealthCheckAll()` — check each active slot, return dead indices
  - `DrainExpired(grace time.Duration) []int` — return slots whose drain deadline passed

**Tests:** `wan_test.go`
- `TestNewWANPool` — create pool, verify all slots empty and ports correct
- `TestPromoteToActive` — start testing, promote, verify state
- `TestMarkDrainingAndKill` — mark draining advance clock past deadline, verify kill
- `TestHealthCheckAll` — mock cmd processes
- `TestDrainNotExpired` — check before deadline, returns empty
- `TestConnCountAtomic` — increment/decrement, verify counter

---

## Step 6: HTTPS proxy + load balancer

**Files:** `proxy.go`

Things to do:
- `startProxy(port int, pool *WANPool, ctx context.Context) error`
  - `http.Server` with `Addr: fmt.Sprintf(":%d", port)`
  - Handler for CONNECT method
  - On CONNECT:
    1. Pick active WAN with fewest connections (iterate pool, compare `connCount` atomically)
    2. Increment that slot's `connCount`
    3. Dial `127.0.0.1:servicePort` via TCP (xray's SOCKS inbound)
    4. Hijack the client connection
    5. `io.Copy` bidirectionally
    6. On disconnect: decrement `connCount`
  - Return error if no active WANs available

**Tests:** `proxy_test.go`
- `TestPickLeastLoaded` — add slots with different conn counts, verify pick
- `TestPickNoActiveWANs` — all slots empty/draining → should return error/no-slot
- `TestConnectionCountTracking` — simulate connect/disconnect, verify count
- `TestMethodFilter` — CONNECT is accepted, GET is rejected with 405

---

## Step 7: main loop orchestration

**Files:** `main.go` (extend), `main_test.go` (integration-style)

Things to do:
- `startup(cfg *Config)`:
  1. Fetch subscription (`http.Get`)
  2. Parse configs
  3. Loop: test configs one by one until `WAN_COUNT` pass `MINIMUM_SPEED`
  4. For each passing config: start service xray on its slot → mark `Active`
  5. Start proxy
- `runLoop(cfg *Config, pool *WANPool, ctx context.Context)`:
  1. Every `FETCH_INTERVAL`:
     - Fetch + parse subscription
     - Test all configs → sort → write `sorted.txt`
     - Pick worst-performing slot below `MINIMUM_SPEED`
     - Begin replacement (drain old, start new)
     - Health-check all slots
  2. `select` on ticker, os signals, and context cancellation
- `gracefulShutdown(pool *WANPool)` — kill all xray processes, close proxy
- Write `sorted.txt` sort format: one line per config, `protocol://server:port speed=Mbps`

**Tests:** `main_test.go`
- `TestStartupPartialSuccess` — mock fewer configs than WAN_COUNT, verify startup retries
- `TestStartupFullSuccess` — enough configs pass, proxy starts
- `TestRunLoopConfigUpdate` — faster config available, replacement triggered
- `TestRunLoopNoChange` — existing configs above MINIMUM_SPEED, no replacement
- `TestGracefulShutdown` — verify all xray processes killed
- `TestSortedTxtWritten` — verify file content matches expected format
