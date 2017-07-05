package bench

import (
	"context"
	"fmt"
	"time"
)

// RandomQuery queries randomly and deterministically based on a seed.
type RandomQuery struct {
	HasClient
	Name          string   `json:"name"`
	MaxDepth      int      `json:"max-depth"`
	MaxArgs       int      `json:"max-args"`
	MaxN          int      `json:"max-n"`
	BaseBitmapID  int64    `json:"base-bitmap-id"`
	BitmapIDRange int64    `json:"bitmap-id-range"`
	Iterations    int      `json:"iterations"`
	Seed          int64    `json:"seed"`
	Frame         string   `json:"frame"`
	Indexes       []string `json:"indexes"`
}

// Init adds the agent num to the random seed and initializes the client.
func (b *RandomQuery) Init(hosts []string, agentNum int) error {
	b.Name = "random-query"
	b.Seed = b.Seed + int64(agentNum)
	return b.HasClient.Init(hosts, agentNum)
}

// Run runs the RandomQuery benchmark
func (b *RandomQuery) Run(ctx context.Context) map[string]interface{} {
	results := make(map[string]interface{})
	if b.client == nil {
		results["error"] = fmt.Errorf("No client set for RandomQuery")
		return results
	}
	qm := NewQueryGenerator(b.Seed)
	qm.IDToFrameFn = func(id uint64) string { return b.Frame }
	s := NewStats()
	var start time.Time
	for n := 0; n < b.Iterations; n++ {
		call := qm.Random(b.MaxN, b.MaxDepth, b.MaxArgs, uint64(b.BaseBitmapID), uint64(b.BitmapIDRange))
		start = time.Now()
		b.ExecuteQuery(b.ContentType, b.Indexes[n%len(b.Indexes)], call.String(), ctx)
		s.Add(time.Now().Sub(start))
	}
	AddToResults(s, results)
	return results
}
