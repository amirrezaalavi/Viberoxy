// Package cands holds the pool of speed-tested proxy configs kept for
// failover: the drop-and-replace API picks replacements from it, and each
// cycle seeds it with the configs it just tested.
package cands

import (
	"sort"
	"sync"
	"time"

	"viberoxy/internal/proxycfg"
)

const (
	// DefaultTTL is how long a tested entry stays in the pool: results
	// older than this are stale (the subscription churns) and get purged.
	DefaultTTL = 30 * time.Minute
	// DefaultMaxPerServer caps how many entries one server:port may hold,
	// so a flaky upstream re-tested under rotating share links cannot
	// flood the pool. The fastest entries win.
	DefaultMaxPerServer = 5
)

// Entry is one pool member: the outcome of a speed test for a single config.
// It mirrors the tester's result struct in package main (which cannot move
// here — the tester stays in main), so call sites convert at the boundary;
// Config is shared by pointer, the value fields are copied.
type Entry struct {
	Config *proxycfg.ProxyConfig
	Speed  float64
	// StabilityScore is the number of distinct exit IPs observed across
	// STABILITY_PROBES probes minus one. The pool carries it for parity
	// with the tester's result but never ranks on it.
	StabilityScore int
	Error          error
	// TestedAt is when this result was measured. Update stamps it from the
	// pool clock when the caller leaves it zero; entries older than the
	// TTL are purged. Callers may set it explicitly for deterministic
	// (clock-free) tests of TTL aging.
	TestedAt time.Time
}

// Pool is a thread-safe pool of top-N working test results,
// sorted by speed descending. It tracks excluded configs so Best() can
// skip them without removing them from the pool.
type Pool struct {
	mu           sync.Mutex
	candidates   []*Entry        // sorted by speed desc (failed results last)
	excluded     map[string]bool // raw URI → excluded
	maxLen       int
	ttl          time.Duration
	maxPerServer int
	now          func() time.Time
}

// NewPool creates a new pool with the given maximum length.
func NewPool(maxLen int) *Pool {
	return &Pool{
		candidates:   make([]*Entry, 0, maxLen),
		excluded:     make(map[string]bool),
		maxLen:       maxLen,
		ttl:          DefaultTTL,
		maxPerServer: DefaultMaxPerServer,
		now:          time.Now,
	}
}

// Update merges new test results into the pool, sorts by speed descending,
// and trims to maxLen. Entries are deduped by Config.Raw: a config that was
// re-tested replaces the stored entry (newest wins) instead of adding one
// row per cycle (F-12), and each server:port keeps at most maxPerServer
// entries so one flaky upstream cannot flood the pool. Stored entries are
// copies stamped with TestedAt (from the pool clock when the caller left it
// zero), and entries older than the TTL are purged first. Thread-safe.
func (p *Pool) Update(results []*Entry) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.purgeLocked()

	// Entries superseded by a config re-tested in this batch.
	retested := make(map[string]bool, len(results))
	for _, e := range results {
		if e != nil && e.Config != nil {
			retested[e.Config.Raw] = true
		}
	}
	merged := make([]*Entry, 0, len(p.candidates)+len(results))
	for _, c := range p.candidates {
		if c.Config != nil && retested[c.Config.Raw] {
			continue // replaced by the newer result below
		}
		merged = append(merged, c)
	}

	// Add the batch; within it the last entry for a Raw wins. Each stored
	// entry is a copy, so callers can keep mutating their own slice.
	seen := make(map[string]int, len(results))
	for _, e := range results {
		if e == nil {
			continue
		}
		cp := *e
		if cp.TestedAt.IsZero() {
			cp.TestedAt = p.now()
		}
		if cp.Config == nil {
			merged = append(merged, &cp)
			continue
		}
		if idx, ok := seen[cp.Config.Raw]; ok {
			merged[idx] = &cp
			continue
		}
		seen[cp.Config.Raw] = len(merged)
		merged = append(merged, &cp)
	}

	// Sort: successful results first (by speed desc), then failed results
	sort.Slice(merged, func(i, j int) bool {
		a, b := merged[i], merged[j]
		if a.Error != nil && b.Error == nil {
			return false
		}
		if a.Error == nil && b.Error != nil {
			return true
		}
		return a.Speed > b.Speed
	})

	// Per-server cap: after the sort the fastest entries come first, so
	// keeping the first N per server:port keeps the fastest N.
	if p.maxPerServer > 0 {
		type serverPort struct {
			server string
			port   int
		}
		counts := make(map[serverPort]int)
		kept := merged[:0]
		for _, c := range merged {
			if c.Config != nil {
				key := serverPort{c.Config.Server, c.Config.Port}
				if counts[key] >= p.maxPerServer {
					continue
				}
				counts[key]++
			}
			kept = append(kept, c)
		}
		merged = kept
	}

	// Trim to maxLen
	if len(merged) > p.maxLen {
		merged = merged[:p.maxLen]
	}
	p.candidates = merged
}

// purgeLocked drops entries older than the TTL. Callers must hold p.mu.
// Update stamps every entry it stores, so no stored entry has a zero
// TestedAt (a zero would read as ancient and be dropped here).
func (p *Pool) purgeLocked() {
	now := p.now()
	kept := p.candidates[:0]
	for _, c := range p.candidates {
		if now.Sub(c.TestedAt) <= p.ttl {
			kept = append(kept, c)
		}
	}
	p.candidates = kept
}

// Exclude marks a config (by its raw URI) as excluded. Thread-safe.
func (p *Pool) Exclude(rawURI string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.excluded[rawURI] = true
}

// Best returns the top non-excluded, non-failed candidate, or nil if
// none remain. exclude (optional, may be nil) is consulted per candidate
// config and skips matches. Thread-safe.
func (p *Pool) Best(exclude func(*proxycfg.ProxyConfig) bool) *Entry {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.purgeLocked()

	for _, c := range p.candidates {
		if c.Error != nil {
			continue
		}
		if c.Config == nil {
			continue // can never be promoted
		}
		if p.excluded[c.Config.Raw] {
			continue
		}
		if exclude != nil && exclude(c.Config) {
			continue
		}
		return c
	}
	return nil
}

// List returns a deep copy of the current pool slice. Mutating the
// returned slice or its elements does not affect the pool. Thread-safe.
func (p *Pool) List() []*Entry {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.purgeLocked()

	out := make([]*Entry, len(p.candidates))
	for i, c := range p.candidates {
		// Shallow copy of Entry is sufficient: Speed and Error are
		// value fields; Config is a pointer but we don't mutate it here.
		copy := *c
		out[i] = &copy
	}
	return out
}

// Len returns the number of candidates currently in the pool. Thread-safe.
func (p *Pool) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.purgeLocked()
	return len(p.candidates)
}
