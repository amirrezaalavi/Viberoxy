//go:build chaos
// +build chaos

package main

// chaosBuildTag reports whether this test binary was built with the chaos
// suite's build tag (`go test -tags chaos`). The untagged build keeps
// T-INT-01 on its short 2-iteration profile; the chaos-tagged build runs
// the full 25-iteration budget (see integrityIterations).
const chaosBuildTag = true
