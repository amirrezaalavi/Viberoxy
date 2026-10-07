// Package cands holds the pool of speed-tested proxy configs kept for
// failover: the drop-and-replace API picks replacements from it, and each
// cycle seeds it with the configs it just tested.
package cands

import (
	"sort"
	"sync"

	"viberoxy/internal/proxycfg"
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
}

// Pool is a thread-safe pool of top-N working test results,
// sorted by speed descending. It tracks excluded configs so Best() can
// skip them without removing them from the pool.
type Pool struct {
	mu         sync.Mutex
	candidates []*Entry        // sorted by speed desc (failed results last)
	excluded   map[string]bool // raw URI → excluded
	maxLen     int
}

// NewPool creates a new pool with the given maximum length.
func NewPool(maxLen int) *Pool {
	return &Pool{
		candidates: make([]*Entry, 0, maxLen),
		excluded:   make(map[string]bool),
		maxLen:     maxLen,
	}
}

// Update merges new test results into the pool, sorts by speed descending,
// and trims to maxLen. Thread-safe.
func (p *Pool) Update(results []*Entry) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Merge new results with existing candidates
	p.candidates = append(p.candidates, results...)

	// Sort: successful results first (by speed desc), then failed results
	sort.Slice(p.candidates, func(i, j int) bool {
		a, b := p.candidates[i], p.candidates[j]
		if a.Error != nil && b.Error == nil {
			return false
		}
		if a.Error == nil && b.Error != nil {
			return true
		}
		return a.Speed > b.Speed
	})

	// Trim to maxLen
	if len(p.candidates) > p.maxLen {
		p.candidates = p.candidates[:p.maxLen]
	}
}

// Exclude marks a config (by its raw URI) as excluded. Thread-safe.
func (p *Pool) Exclude(rawURI string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.excluded[rawURI] = true
}

// Best returns the top non-excluded, non-failed candidate, or nil if
// none remain. Thread-safe.
func (p *Pool) Best() *Entry {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, c := range p.candidates {
		if c.Error != nil {
			continue
		}
		if p.excluded[c.Config.Raw] {
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
	return len(p.candidates)
}
