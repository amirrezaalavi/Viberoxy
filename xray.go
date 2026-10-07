package main

import (
	"fmt"
	"viberoxy/internal/proxycfg"
	"viberoxy/internal/xraycfg"
	"viberoxy/internal/xrayproc"
)

// XrayConfig is the rendered xray JSON document type. It and the rest of the
// config-generation code (BuildXrayConfig's body, buildOutbound, the extract*
// helpers, StreamSettings/Outbound/…) moved to internal/xraycfg in task-1-3;
// this alias keeps package main's tests on the same type name.
type XrayConfig = xraycfg.XrayConfig

// BuildXrayConfig renders the xray JSON for one proxy config via
// internal/xraycfg. When muxEnabled is omitted it defaults to true: client
// connections to this xray instance then share a single multiplexed upstream
// connection, amortizing the TLS/protocol handshake per connection. Mux is
// only emitted on proxied outbounds (never on the freedom fallback, never on
// socks5 outbounds whose plain-proxy remote cannot demultiplex smux, and never
// when the config carries a flow such as xtls-rprx-vision).
//
// Note: production call sites never omit muxEnabled — they pass the parsed
// proxycfg.Config.XrayMux (XRAY_MUX) explicitly; the omitted default exists
// for tests and callers with no config at hand.
func BuildXrayConfig(cfg *proxycfg.ProxyConfig, inboundPort int, muxEnabled ...bool) ([]byte, error) {
	return xraycfg.BuildXrayConfig(cfg, inboundPort, muxEnabled...)
}

// StartXray renders the xray config for cfg and starts the process under a
// lifecycle handle (see internal/xrayproc). The second result is the temp
// config file the child was started with.
func StartXray(cfg *proxycfg.ProxyConfig, inboundPort int, muxEnabled ...bool) (*xrayproc.Handle, string, error) {
	configBytes, err := BuildXrayConfig(cfg, inboundPort, muxEnabled...)
	if err != nil {
		return nil, "", fmt.Errorf("build xray config: %w", err)
	}

	h, err := xrayproc.Start(configBytes)
	if err != nil {
		return nil, "", err
	}
	return h, h.ConfigPath(), nil
}

// StopXray stops the process behind h and removes configPath.
func StopXray(h *xrayproc.Handle, configPath string) error {
	return h.Stop(configPath)
}

// HealthCheckXray reports whether the xray process behind h is alive.
func HealthCheckXray(h *xrayproc.Handle) bool {
	return h.Alive()
}
