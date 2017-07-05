package bench

import (
	"fmt"

	"context"
	"time"
)

// DiagonalSetBits sets bits with increasing profile id and bitmap id.
type DiagonalSetBits struct {
	HasClient
	Name          string `json:"name"`
	BaseBitmapID  int    `json:"base-bitmap-id"`
	BaseProfileID int    `json:"base-profile-id"`
	Iterations    int    `json:"iterations"`
	Index         string `json:"index"`
	Frame         string `json:"frame"`
}

// Init sets up the pilosa client and modifies the configured values based on
// the agent num.
func (b *DiagonalSetBits) Init(hosts []string, agentNum int) error {
	b.Name = "diagonal-set-bits"
	b.BaseBitmapID = b.BaseBitmapID + (agentNum * b.Iterations)
	b.BaseProfileID = b.BaseProfileID + (agentNum * b.Iterations)
	err := initIndex(hosts[0], b.Index, b.Frame)
	if err != nil {
		return err
	}
	return b.HasClient.Init(hosts, agentNum)
}

// Run runs the DiagonalSetBits benchmark
func (b *DiagonalSetBits) Run(ctx context.Context) map[string]interface{} {
	results := make(map[string]interface{})
	if b.client == nil {
		results["error"] = fmt.Errorf("No client set for DiagonalSetBits")
		return results
	}
	s := NewStats()
	var start time.Time
	for n := 0; n < b.Iterations; n++ {
		query := fmt.Sprintf("SetBit(frame='%s', rowID=%d, columnID=%d)", b.Frame, b.BaseBitmapID+n, b.BaseProfileID+n)
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
