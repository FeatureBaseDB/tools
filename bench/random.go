package bench

import (
	"context"
	"fmt"
	"math/rand"
	"time"
)

// RandomSetBits sets bits randomly and deterministically based on a seed.
type RandomSetBits struct {
	HasClient
	Name          string `json:"name"`
	MinRowID      int64  `json:"min-row-id"`
	MaxRowID      int64  `json:"max-row-id"`
	MinColumnID   int64  `json:"min-column-id"`
	MaxColumnID   int64  `json:"max-column-id"`
	Iterations    int    `json:"iterations"`
	BatchSize     int    `json:"batch-size"`
	Seed          int64  `json:"seed"`
	NumAttrs      int    `json:"num-attrs"`
	NumAttrValues int    `json:"num-attr-values"`
	Index         string `json:"index"`
	Frame         string `json:"frame"`
}

// Init adds the agent num to the random seed and initializes the client.
func (b *RandomSetBits) Init(hostSetup *HostSetup, agentNum int) error {
	b.Name = "random-set-bits"
	b.Seed = b.Seed + int64(agentNum)
	err := b.HasClient.Init(hostSetup, agentNum)
	if err != nil {
		return err
	}
	if b.BatchSize <= 0 {
		return fmt.Errorf("batch size must be greater than 0, currently: %d", b.BatchSize)
	}
	return b.InitIndex(b.Index, b.Frame)
}

// Run runs the RandomSetBits benchmark
func (b *RandomSetBits) Run(ctx context.Context) *Result {
	src := rand.NewSource(b.Seed)
	rng := rand.New(src)
	results := NewResult()
	if b.client == nil {
		results.err = fmt.Errorf("No client set for RandomSetBits")
		return results
	}
	n := 0
	for n < b.Iterations {
		queries := ""
		for i := 0; i < b.BatchSize && n < b.Iterations; i++ {
			n++
			rowID := rng.Int63n(b.MaxRowID - b.MinRowID)
			profID := rng.Int63n(b.MaxColumnID - b.MinColumnID)
			query := fmt.Sprintf("SetBit(frame='%s', rowID=%d, columnID=%d)", b.Frame, b.MinRowID+rowID, b.MinColumnID+profID)

			queries = queries + query + b.getAttrQuery(rng, rowID)
		}
		start := time.Now()
		_, err := b.ExecuteQuery(ctx, b.Index, queries)
		results.Add(time.Since(start), nil)
		if err != nil {
			results.err = err
			return results
		}
	}
	return results
}

var letters = "abcdefghijklmnopqrstuvwxyz"

func (b *RandomSetBits) getAttrQuery(rng *rand.Rand, rowID int64) string {
	if b.NumAttrs <= 0 || b.NumAttrValues <= 0 {
		return ""
	}
	attri := rng.Intn(b.NumAttrs)
	val := rng.Intn(b.NumAttrValues)
	return fmt.Sprintf("SetRowAttrs(rowID=%d, frame='%s', %c%d=%d)", rowID, b.Frame, letters[attri%len(letters)], attri, val)

}
