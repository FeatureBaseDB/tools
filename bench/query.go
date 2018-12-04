package bench

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/pilosa/go-pilosa"
)

type QueryBenchmark struct {
	Name       string `json:"name"`
	Query      string `json:"query"`
	Index      string `json:"index"`
	Iterations int    `json:"iterations"`

	Logger *log.Logger `json:"-"`
}

func NewQueryBenchmark() *QueryBenchmark {
	return &QueryBenchmark{
		Name:   "query",
		Logger: log.New(os.Stderr, "", log.LstdFlags),
	}
}

// Run runs the benchmark.
func (b *QueryBenchmark) Run(ctx context.Context, client *pilosa.Client, agentNum int) (*Result, error) {
	result := NewResult()
	result.AgentNum = agentNum
	result.Configuration = b

	// Initialize schema.
	index, _, err := ensureSchema(client, b.Index, "")
	if err != nil {
		return result, err
	}

	for n := 0; n < b.Iterations; n++ {
		start := time.Now()
		resp, err := client.Query(index.RawQuery(b.Query))
		result.Add(time.Since(start), resp)
		if err != nil {
			return result, err
		}
	}
	return result, nil
}
