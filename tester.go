package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"time"
)

type TestResult struct {
	Config *ProxyConfig
	Speed  float64
	Error  error
}

func TestSpeed(cfg *ProxyConfig, testPort int, timeout time.Duration, downloadURL string, downloadSize int64) *TestResult {
	cmd, configPath, err := StartXray(cfg, testPort)
	if err != nil {
		return &TestResult{Config: cfg, Error: fmt.Errorf("start xray: %w", err)}
	}
	defer StopXray(cmd, configPath)

	socksAddr := fmt.Sprintf("127.0.0.1:%d", testPort)
	speed, err := DownloadMeasurer(socksAddr, downloadURL, downloadSize, timeout)
	if err != nil {
		return &TestResult{Config: cfg, Error: err}
	}
	return &TestResult{Config: cfg, Speed: speed}
}

func TestAll(configs []*ProxyConfig, testPortBase int, timeout time.Duration, downloadURL string, downloadSize int64) []*TestResult {
	results := make([]*TestResult, len(configs))
	for i, cfg := range configs {
		results[i] = TestSpeed(cfg, testPortBase+i, timeout, downloadURL, downloadSize)
	}
	return SortResults(results)
}

func SortResults(results []*TestResult) []*TestResult {
	sorted := make([]*TestResult, len(results))
	copy(sorted, results)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Error != nil && sorted[j].Error == nil {
			return false
		}
		if sorted[i].Error == nil && sorted[j].Error != nil {
			return true
		}
		return sorted[i].Speed > sorted[j].Speed
	})
	return sorted
}

func DownloadMeasurer(socksAddr string, downloadURL string, downloadSize int64, timeout time.Duration) (float64, error) {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return socks5Dial(ctx, socksAddr, addr)
		},
	}
	client := &http.Client{
		Transport: transport,
	}
	defer client.CloseIdleConnections()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", downloadURL, nil)
	if err != nil {
		return 0, fmt.Errorf("create request: %w", err)
	}

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("http get: %w", err)
	}
	defer resp.Body.Close()

	var reader io.Reader = resp.Body
	if downloadSize > 0 {
		reader = io.LimitReader(resp.Body, downloadSize)
	}

	n, err := io.Copy(io.Discard, reader)
	if err != nil {
		return 0, fmt.Errorf("read body: %w", err)
	}

	elapsed := time.Since(start).Seconds()
	if elapsed <= 0 {
		elapsed = 0.001
	}

	return float64(n*8) / elapsed / 1_000_000, nil
}

func socks5Dial(ctx context.Context, socksAddr, targetAddr string) (net.Conn, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", socksAddr)
	if err != nil {
		return nil, fmt.Errorf("connect to socks: %w", err)
	}

	if _, err := conn.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		conn.Close()
		return nil, fmt.Errorf("auth write: %w", err)
	}

	buf := make([]byte, 2)
	if _, err := io.ReadFull(conn, buf); err != nil {
		conn.Close()
		return nil, fmt.Errorf("auth read: %w", err)
	}
	if buf[0] != 0x05 || buf[1] != 0x00 {
		conn.Close()
		return nil, fmt.Errorf("auth failed: ver=%d status=%d", buf[0], buf[1])
	}

	host, portStr, err := net.SplitHostPort(targetAddr)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("split host port: %w", err)
	}

	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		conn.Close()
		return nil, fmt.Errorf("invalid port: %s", portStr)
	}

	ip := net.ParseIP(host)
	var req []byte
	if ip != nil && ip.To4() != nil {
		ip4 := ip.To4()
		req = []byte{0x05, 0x01, 0x00, 0x01, ip4[0], ip4[1], ip4[2], ip4[3], byte(port >> 8), byte(port)}
	} else if ip != nil {
		ip16 := ip.To16()
		req = append([]byte{0x05, 0x01, 0x00, 0x04}, ip16...)
		req = append(req, byte(port>>8), byte(port))
	} else {
		if len(host) > 255 {
			conn.Close()
			return nil, fmt.Errorf("hostname too long: %d", len(host))
		}
		req = append([]byte{0x05, 0x01, 0x00, 0x03, byte(len(host))}, []byte(host)...)
		req = append(req, byte(port>>8), byte(port))
	}

	if _, err := conn.Write(req); err != nil {
		conn.Close()
		return nil, fmt.Errorf("connect write: %w", err)
	}

	resp := make([]byte, 4)
	if _, err := io.ReadFull(conn, resp); err != nil {
		conn.Close()
		return nil, fmt.Errorf("connect response: %w", err)
	}
	if resp[0] != 0x05 || resp[1] != 0x00 {
		conn.Close()
		return nil, fmt.Errorf("connect failed: ver=%d rep=%d", resp[0], resp[1])
	}

	switch resp[3] {
	case 0x01:
		rest := make([]byte, 6)
		if _, err := io.ReadFull(conn, rest); err != nil {
			conn.Close()
			return nil, fmt.Errorf("bind addr ipv4: %w", err)
		}
	case 0x03:
		lenByte := make([]byte, 1)
		if _, err := io.ReadFull(conn, lenByte); err != nil {
			conn.Close()
			return nil, fmt.Errorf("bind domain len: %w", err)
		}
		rest := make([]byte, int(lenByte[0])+2)
		if _, err := io.ReadFull(conn, rest); err != nil {
			conn.Close()
			return nil, fmt.Errorf("bind domain: %w", err)
		}
	case 0x04:
		rest := make([]byte, 18)
		if _, err := io.ReadFull(conn, rest); err != nil {
			conn.Close()
			return nil, fmt.Errorf("bind addr ipv6: %w", err)
		}
	default:
		conn.Close()
		return nil, fmt.Errorf("unknown atyp: %d", resp[3])
	}

	return conn, nil
}
