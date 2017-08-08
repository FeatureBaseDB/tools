package bench

import (
	"fmt"

	"context"
	"time"
)

// DiagonalSetBits sets bits with increasing column id and row id.
type DiagonalSetBits struct {
	HasClient
	Name        string `json:"name"`
	MinRowID    int    `json:"min-row-id"`
	MinColumnID int    `json:"min-column-id"`
	Iterations  int    `json:"iterations"`
	Index       string `json:"index"`
	Frame       string `json:"frame"`
}

// Init sets up the pilosa client and modifies the configured values based on
// the agent num.
func (b *DiagonalSetBits) Init(hosts []string, agentNum int) error {
	b.Name = "diagonal-set-bits"
	b.MinRowID = b.MinRowID + (agentNum * b.Iterations)
	b.MinColumnID = b.MinColumnID + (agentNum * b.Iterations)
	err := b.HasClient.Init(hosts, agentNum)
	if err != nil {
		return err
	}
	return b.InitIndex(b.Index, b.Frame)
}

// Run runs the DiagonalSetBits benchmark
func (b *DiagonalSetBits) Run(ctx context.Context) *Result {
	results := NewResult()
	if b.client == nil {
		results.err = fmt.Errorf("No client set for DiagonalSetBits")
		return results
	}
	var start time.Time
	for n := 0; n < b.Iterations; n++ {
		query := fmt.Sprintf("SetBit(frame='%s', rowID=%d, columnID=%d)", b.Frame, b.MinRowID+n, b.MinColumnID+n)
		start = time.Now()
		_, err := b.ExecuteQuery(ctx, b.Index, query)
		results.Add(time.Since(start), nil)
		if err != nil {
			results.err = err
			return results
		}
	}
	return results
}
