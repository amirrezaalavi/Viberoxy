package main

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// WANSlotInfo is the JSON-serializable view of a WAN slot's state,
// exposed via the /api/viberoxy/wans endpoint.
type WANSlotInfo struct {
	Index            int     `json:"index"`
	State            string  `json:"state"`
	SpeedMbps        float64 `json:"speed_mbps"`
	Conns            int64   `json:"conns"`
	ExitIP           string  `json:"exit_ip"`
	LastProbe        string  `json:"last_probe"`
	ConsecutiveFails int64   `json:"consecutive_fails"`
}

// CycleTimingInfo is the JSON view of the most recent cycle and the next
// scheduled cycle. Timestamps are RFC3339Nano strings; they are empty until
// the corresponding time is known.
type CycleTimingInfo struct {
	LastCycle        string `json:"last_cycle"`
	NextCycle        string `json:"next_cycle"`
	SecondsUntilNext int64  `json:"seconds_until_next"`
}

func formatCycleTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339Nano)
}

// handleGetCycleTiming exposes cycle timestamps and a countdown suitable for
// the control-plane UI.
func handleGetCycleTiming() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		last, next := cycleTimingSnapshot()
		secondsUntilNext := int64(0)
		if !next.IsZero() {
			secondsUntilNext = int64(time.Until(next).Seconds())
			if secondsUntilNext < 0 {
				secondsUntilNext = 0
			}
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(CycleTimingInfo{
			LastCycle:        formatCycleTime(last),
			NextCycle:        formatCycleTime(next),
			SecondsUntilNext: secondsUntilNext,
		})
	}
}

// handleTriggerCycle queues an immediate cycle without waiting for a current
// cycle to finish. The one-element channel coalesces repeated requests.
func handleTriggerCycle() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		queued := false
		select {
		case triggerCycle <- struct{}{}:
			queued = true
		default:
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "accepted",
			"queued": queued,
		})
	}
}

// NewAPIHandler returns the viberoxy control API, including WAN state,
// WAN drop/replacement, cycle timing, the manual cycle trigger, and the
// candidate pool. A Config is supplied by startup; it is variadic to preserve
// compatibility with callers that only use the read-only endpoints.
func NewAPIHandler(pool *WANPool, configs ...*Config) http.Handler {
	var cfg *Config
	if len(configs) > 0 {
		cfg = configs[0]
	}
	dropOpts := DropAndReplaceOptions{Candidates: candidatePool}
	if cfg != nil {
		dropOpts.TestPort = cfg.TestBasePort
		dropOpts.Timeout = time.Duration(cfg.TestTimeout) * time.Second
		dropOpts.DownloadURL = buildDownloadURL(cfg, cfg.DownloadSize)
		dropOpts.DownloadSize = cfg.DownloadSize
		dropOpts.StabilityProbes = cfg.StabilityProbes
		dropOpts.XrayMux = cfg.XrayMux
	}

	mux := http.NewServeMux()
	mux.Handle("/api/viberoxy/wans", handleGetWANSlots(pool))
	mux.Handle("/api/viberoxy/wans/", handleDropWAN(pool, dropOpts, triggerCycle))
	mux.Handle("/api/viberoxy/cycle", handleGetCycleTiming())
	mux.Handle("/api/viberoxy/cycle/trigger", handleTriggerCycle())
	mux.Handle("/api/viberoxy/candidates", handleGetCandidates())
	return mux
}

// dropWANResponse is returned by the drop endpoint for every application-level
// outcome. WAN is present only when a replacement was activated.
type dropWANResponse struct {
	Status  string       `json:"status"`
	Message string       `json:"message,omitempty"`
	WAN     *WANSlotInfo `json:"wan,omitempty"`
}

func writeDropWANResponse(w http.ResponseWriter, statusCode int, response dropWANResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(response)
}

func parseDropWANIndex(path string) (int, error) {
	const prefix = "/api/viberoxy/wans/"
	if !strings.HasPrefix(path, prefix) {
		return 0, errors.New("invalid drop WAN path")
	}
	parts := strings.Split(strings.TrimPrefix(path, prefix), "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] != "drop" {
		return 0, errors.New("invalid drop WAN path")
	}
	index, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, errors.New("invalid WAN index")
	}
	return index, nil
}

// handleDropWAN drops one WAN and immediately promotes the best cached
// candidate after re-testing it. When the candidate pool has no eligible
// entries, it queues a fresh cycle and leaves the slot empty for that cycle to
// refill.
func handleDropWAN(pool *WANPool, opts DropAndReplaceOptions, cycleTrigger chan<- struct{}) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeDropWANResponse(w, http.StatusMethodNotAllowed, dropWANResponse{
				Status:  "error",
				Message: "method not allowed",
			})
			return
		}

		index, err := parseDropWANIndex(r.URL.Path)
		if err != nil || index < 0 || index >= len(pool.Slots) {
			writeDropWANResponse(w, http.StatusBadRequest, dropWANResponse{
				Status:  "error",
				Message: "invalid WAN index",
			})
			return
		}

		_, err = pool.DropAndReplace(index, opts)
		switch {
		case errors.Is(err, ErrNoReplacementCandidate):
			select {
			case cycleTrigger <- struct{}{}:
			default:
			}
			writeDropWANResponse(w, http.StatusAccepted, dropWANResponse{
				Status:  "replacing",
				Message: "no candidates, fetching...",
			})
		case errors.Is(err, ErrReplacementTestFailed):
			slog.Warn("WAN replacement candidate failed re-test", "index", index, "error", err)
			writeDropWANResponse(w, http.StatusBadGateway, dropWANResponse{
				Status:  "error",
				Message: "replacement failed test",
			})
		case err != nil:
			slog.Error("WAN replacement failed", "index", index, "error", err)
			writeDropWANResponse(w, http.StatusBadGateway, dropWANResponse{
				Status:  "error",
				Message: "replacement failed to start",
			})
		default:
			info := wanSlotInfo(pool, index)
			writeDropWANResponse(w, http.StatusOK, dropWANResponse{
				Status: "replaced",
				WAN:    &info,
			})
		}
	}
}

// handleGetCandidates returns the current candidate pool as a JSON array.
// Each entry carries the config identity and measured speed/error so the
// UI can surface replacement candidates.
func handleGetCandidates() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if candidatePool == nil {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte("[]"))
			return
		}
		type candidateView struct {
			Name     string  `json:"name"`
			Server   string  `json:"server"`
			Port     int     `json:"port"`
			Protocol string  `json:"protocol"`
			Raw      string  `json:"raw"`
			Speed    float64 `json:"speed_mbps"`
			Error    string  `json:"error,omitempty"`
		}
		list := candidatePool.List()
		view := make([]candidateView, 0, len(list))
		for _, c := range list {
			cv := candidateView{
				Server:   c.Config.Server,
				Port:     c.Config.Port,
				Protocol: c.Config.Protocol,
				Raw:      c.Config.Raw,
				Speed:    c.Speed,
			}
			if c.Config.Name != "" {
				cv.Name = c.Config.Name
			}
			if c.Error != nil {
				cv.Error = c.Error.Error()
			}
			view = append(view, cv)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(view)
	}
}

// wanSlotInfo returns a consistent JSON view of one slot.
func wanSlotInfo(pool *WANPool, index int) WANSlotInfo {
	slot := pool.Slots[index]
	slot.mu.Lock()
	state := slot.State
	speed := slot.SpeedMbps
	exitIP := slot.ExitIP
	lastProbe := slot.LastProbe
	slot.mu.Unlock()

	return WANSlotInfo{
		Index:            index,
		State:            state.String(),
		SpeedMbps:        speed,
		Conns:            atomic.LoadInt64(&slot.ConnCount),
		ExitIP:           exitIP,
		LastProbe:        lastProbe.Format(time.RFC3339),
		ConsecutiveFails: atomic.LoadInt64(&slot.ConsecutiveFails),
	}
}

// handleGetWANSlots returns a JSON array of WANSlotInfo for every slot
// in the pool. The handler is safe for concurrent use: it reads each
// slot under its mutex and uses atomic loads for the counters.
func handleGetWANSlots(pool *WANPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		infos := make([]WANSlotInfo, 0, len(pool.Slots))
		for i := range pool.Slots {
			infos = append(infos, wanSlotInfo(pool, i))
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(infos)
	}
}
