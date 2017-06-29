package bench

import (
	"fmt"

	"context"
	"math/rand"
	"time"
)

// RandomSetBits sets bits randomly and deterministically based on a seed.
type RandomSetBits struct {
	HasClient
	Name           string `json:"name"`
	BaseBitmapID   int64  `json:"base-bitmap-id"`
	BaseProfileID  int64  `json:"base-profile-id"`
	BitmapIDRange  int64  `json:"bitmap-id-range"`
	ProfileIDRange int64  `json:"profile-id-range"`
	Iterations     int    `json:"iterations"`
	Seed           int64  `json:"seed"`
	Index          string `json:"index"`
	Frame          string `json:"frame"`
}

// Init adds the agent num to the random seed and initializes the client.
func (b *RandomSetBits) Init(hosts []string, agentNum int) error {
	b.Name = "random-set-bits"
	b.Seed = b.Seed + int64(agentNum)
	err := initIndex(hosts[0], b.Index, b.Frame)
	if err != nil {
		return err
	}
	return b.HasClient.Init(hosts, agentNum)
}

// Run runs the RandomSetBits benchmark
func (b *RandomSetBits) Run(ctx context.Context) map[string]interface{} {
	src := rand.NewSource(b.Seed)
	rng := rand.New(src)
	results := make(map[string]interface{})
	if b.client == nil {
		results["error"] = fmt.Errorf("No client set for RandomSetBits")
		return results
	}
	s := NewStats()
	var start time.Time
	for n := 0; n < b.Iterations; n++ {
		bitmapID := rng.Int63n(b.BitmapIDRange)
		profID := rng.Int63n(b.ProfileIDRange)
		query := fmt.Sprintf("SetBit(frame='%s', rowID=%d, columnID=%d)", b.Frame, b.BaseBitmapID+bitmapID, b.BaseProfileID+profID)
		start = time.Now()
		_, err := b.client.ExecuteQuery(ctx, b.Index, query, true)
		if err != nil {
			results["error"] = err.Error()
			return results
		}
		s.Add(time.Now().Sub(start))
	}
	AddToResults(s, results)
	return results
}
