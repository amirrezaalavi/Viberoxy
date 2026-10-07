package xraycfg

import "encoding/json"

// The rendered xray JSON document types. Extracted from package main's
// xray.go (task-1-3) unchanged: field names, JSON tags and semantics are
// what they were before the move.

type XrayConfig struct {
	Log       LogConfig        `json:"log"`
	Inbounds  []InboundConfig  `json:"inbounds"`
	Outbounds []OutboundConfig `json:"outbounds"`
}

type LogConfig struct {
	LogLevel string `json:"loglevel"`
}

type InboundConfig struct {
	Port     int             `json:"port"`
	Listen   string          `json:"listen"`
	Protocol string          `json:"protocol"`
	Settings json.RawMessage `json:"settings"`
}

type OutboundConfig struct {
	Protocol       string          `json:"protocol"`
	Settings       json.RawMessage `json:"settings"`
	StreamSettings *StreamSettings `json:"streamSettings,omitempty"`
	Mux            *MuxConfig      `json:"mux,omitempty"`
}

type MuxConfig struct {
	Enabled     bool `json:"enabled"`
	Concurrency int  `json:"concurrency,omitempty"`
}

type StreamSettings struct {
	Network         string           `json:"network"`
	Security        string           `json:"security"`
	TLSSettings     *TLSSettings     `json:"tlsSettings,omitempty"`
	WSSettings      *WSSettings      `json:"wsSettings,omitempty"`
	GRPCSettings    *GRPCSettings    `json:"grpcSettings,omitempty"`
	TCPSettings     *TCPSettings     `json:"tcpSettings,omitempty"`
	RealitySettings *RealitySettings `json:"realitySettings,omitempty"`
}

type TLSSettings struct {
	ServerName    string   `json:"serverName,omitempty"`
	Fingerprint   string   `json:"fingerprint,omitempty"`
	ALPN          []string `json:"alpn,omitempty"`
	AllowInsecure bool     `json:"allowInsecure,omitempty"`
}

type WSSettings struct {
	Path    string            `json:"path,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
}

type GRPCSettings struct {
	ServiceName string `json:"serviceName,omitempty"`
}

type TCPSettings struct {
	Header *TCPHeader `json:"header,omitempty"`
}

type TCPHeader struct {
	Type    string       `json:"type"`
	Request *HTTPRequest `json:"request,omitempty"`
}

type HTTPRequest struct {
	Version string              `json:"version,omitempty"`
	Method  string              `json:"method,omitempty"`
	Path    []string            `json:"path,omitempty"`
	Headers map[string][]string `json:"headers,omitempty"`
}

type RealitySettings struct {
	ServerName  string `json:"serverName,omitempty"`
	Fingerprint string `json:"fingerprint,omitempty"`
	PublicKey   string `json:"publicKey,omitempty"`
	ShortID     string `json:"shortId,omitempty"`
	SpiderX     string `json:"spiderX,omitempty"`
}
