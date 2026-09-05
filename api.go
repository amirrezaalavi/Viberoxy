package main

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"
)

// WANSlotInfo is the JSON-serializable view of a WAN slot's state,
// exposed via the /api/viberoxy/wans endpoint.
type WANSlotInfo struct {
	Index             int     `json:"index"`
	State             string  `json:"state"`
	SpeedMbps         float64 `json:"speed_mbps"`
	Conns             int64   `json:"conns"`
	ExitIP            string  `json:"exit_ip"`
	LastProbe         string  `json:"last_probe"`
	ConsecutiveFails  int64   `json:"consecutive_fails"`
}

// handleGetWANSlots returns a JSON array of WANSlotInfo for every slot
// in the pool. The handler is safe for concurrent use: it reads each
// slot under its mutex and uses atomic loads for the counters.
func handleGetWANSlots(pool *WANPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		infos := make([]WANSlotInfo, 0, len(pool.Slots))
		for i, slot := range pool.Slots {
			slot.mu.Lock()
			state := slot.State
			speed := slot.SpeedMbps
			exitIP := slot.ExitIP
			lastProbe := slot.LastProbe
			slot.mu.Unlock()

			infos = append(infos, WANSlotInfo{
				Index:            i,
				State:            state.String(),
				SpeedMbps:        speed,
				Conns:            atomic.LoadInt64(&slot.ConnCount),
				ExitIP:           exitIP,
				LastProbe:        lastProbe.Format(time.RFC3339),
				ConsecutiveFails: atomic.LoadInt64(&slot.ConsecutiveFails),
			})
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(infos)
	}
}