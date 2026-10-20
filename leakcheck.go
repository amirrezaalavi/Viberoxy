package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"viberoxy/internal/proxycfg"
)

// Non-leak evidence.
//
// buildOutbound no longer falls back to a "freedom" (direct) outbound for
// protocols xray cannot render, so a promoted candidate always carries its
// traffic through a real tunnel. This file adds the INDEPENDENT proof: the
// exit IP this machine itself has (measured without any tunnel) is
// remembered at startup, and a candidate whose observed exit IP is the same
// address is reported.
//
// The check is ADVISORY by default because the two cases are
// indistinguishable from the exit IP alone:
//   - the candidate really did leak to a direct egress, or
//   - the candidate's server is reachable under this machine's own public
//     address (self-hosted proxy, same NAT) — a perfectly good tunnel.
//
// REJECT_DIRECT_EXIT=true turns the report into a hard rejection for
// operators who know their candidates are all remote.
var (
	// directExitIP is this machine's own exit address, measured once at
	// startup. Empty when it could not be measured (no direct egress), in
	// which case the check simply has nothing to compare against.
	directExitIP string
	// rejectDirectExit is set from REJECT_DIRECT_EXIT at startup.
	rejectDirectExit bool
)

// measureDirectExitIP returns the exit IP of THIS machine: a plain HTTP GET
// to the exit-IP endpoint with the proxy explicitly disabled, so the answer
// is the address unrouted traffic leaves from. It returns "" when the
// measurement is not possible (no direct egress, endpoint blocked).
func measureDirectExitIP(timeout time.Duration) string {
	transport := &http.Transport{
		// Explicitly no proxy: the point is to learn the DIRECT address.
		Proxy: nil,
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: timeout}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ProbeWANURL, nil)
	if err != nil {
		return ""
	}
	resp, err := client.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ""
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(body))
}

// checkDirectEgress reports whether a tested candidate egresses with this
// machine's own exit IP. It logs the finding and returns true when the
// candidate must NOT be promoted (REJECT_DIRECT_EXIT=true); otherwise the
// caller keeps it and the finding is advisory.
func checkDirectEgress(stage string, result *TestResult) bool {
	if result == nil || result.ExitIP == "" || directExitIP == "" {
		return false
	}
	if result.ExitIP != directExitIP {
		return false
	}
	server := ""
	if result.Config != nil {
		server = result.Config.Server
	}
	slog.Warn("candidate egresses with this machine's own exit ip: the tunnel may not be carrying the traffic",
		"stage", stage, "server", server, "exit_ip", result.ExitIP,
		"reject", rejectDirectExit)
	return rejectDirectExit
}

// initDirectEgressCheck measures this machine's exit IP and records whether
// a match must be rejected. Called once at startup.
func initDirectEgressCheck(cfg *proxycfg.Config) {
	if cfg != nil {
		rejectDirectExit = cfg.RejectDirectExit
	}
	directExitIP = measureDirectExitIP(10 * time.Second)
	if directExitIP != "" {
		slog.Info("direct exit ip measured", "ip", directExitIP,
			"reject_matching_candidates", rejectDirectExit)
	} else {
		slog.Warn("direct exit ip not measurable: the leak check stays advisory (no comparison address)")
	}
}
