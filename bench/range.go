package bench

import (
	"context"
	"fmt"
	"math/rand"
	"time"
)

// RangeQuery runs Range query randomly
type RangeQuery struct {
	HasClient
	Name       string   `json:"name"`
	MaxDepth   int      `json:"max-depth"`
	MaxArgs    int      `json:"max-args"`
	MaxN       int      `json:"max-n"`
	MinRange   int64    `json:"min-range"`
	MaxRange   int64    `json:"max-range"`
	Iterations int      `json:"iterations"`
	Seed       int64    `json:"seed"`
	Frame      string   `json:"frame"`
	Index      string   `json:"index"`
	Fields     []string `json:"field"`
	QueryType  string   `json:"type"`
}

// Init adds the agent num to the random seed and initializes the client.
func (b *RangeQuery) Init(hosts []string, agentNum int) error {
	b.Name = "range-query"
	b.Seed = b.Seed + int64(agentNum)
	return b.HasClient.Init(hosts, agentNum)
}

// Run runs the RandomQuery benchmark
func (b *RangeQuery) Run(ctx context.Context) *Result {
	src := rand.NewSource(b.Seed)
	rng := rand.New(src)
	results := NewResult()
	if b.client == nil {
		results.err = fmt.Errorf("No client set for RangeQuery")
		return results
	}
	qm := NewQueryGenerator(b.Seed)
	qm.IDToFrameFn = func(id uint64) string { return b.Frame }
	qm.Frames = []string{b.Frame}

	var start time.Time
	for n := 0; n < b.Iterations; n++ {
		var field int
		if len(b.Fields) > 1 {
			field = rng.Intn(len(b.Fields) - 1)
		} else {
			field = 0
		}
		call := qm.RandomRangeQuery(b.MaxDepth, b.MaxArgs, b.Frame, b.Fields[field], uint64(b.MinRange), uint64(b.MaxRange))

		start = time.Now()
		_, err := b.ExecuteQuery(ctx, b.Index, call.String())
		results.Add(time.Since(start), nil)
		if err != nil {
			results.err = fmt.Errorf("Executing '%s', err: %v", call.String(), err)
			return results
		}
	}
	return results
}
