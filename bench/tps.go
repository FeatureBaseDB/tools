package bench

import (
	"context"
	"log"
	"math/rand"
	"os"
	"runtime"
	"time"

	"github.com/pilosa/go-pilosa"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type TPSBenchmark struct {
	Name        string   `json:"name"`
	Intersect   bool     `json:"intersect" help:"If true, include Intersect queries in benchmark."`
	Union       bool     `json:"union" help:"If true, include Union queries in benchmark."`
	Difference  bool     `json:"difference" help:"If true, include Difference queries in benchmark."`
	Xor         bool     `json:"xor" help:"If true, include XOR queries in benchmark."`
	Fields      []string `json:"fields" help:"Comma separated list of fields. If blank, use all fields in index schema."`
	MinRowID    int64    `json:"min-row-id" help:"Minimum row ID to use in queries."`
	MaxRowID    int64    `json:"max-row-id" help:"Max row ID to use in queries. If 0, determine max available."`
	Index       string   `json:"index" help:"Index to use. If blank, one is chosen randomly from the schema."`
	Concurrency int      `json:"concurrency" help:"Run this many goroutines concurrently." short:"y"`
	Iterations  int      `json:"iterations" help:"Each goroutine will perform this many queries."`

	// Complexity int `help:"Number of Rows calls to include in each query."`
	// Depth int `help:"Nesting depth of queries. (e.g. Xor(Row(blah=2), Intersect(Row(ha=3), Row(blah=4))))"`

	Logger *log.Logger `json:"-"`
}

func NewTPSBenchmark() *TPSBenchmark {
	return &TPSBenchmark{
		Name:        "tps",
		Intersect:   true,
		Concurrency: runtime.NumCPU(),
		MaxRowID:    100,
		Iterations:  1000,
		Logger:      log.New(os.Stderr, "", log.LstdFlags),
	}
}

// Run runs the benchmark.
func (b *TPSBenchmark) Run(ctx context.Context, client *pilosa.Client, agentNum int) (*Result, error) {
	result := NewResult()
	result.AgentNum = agentNum
	result.Configuration = b

	// get the schema to validate existence of index/fields or pick ones to use.
	s, err := client.Schema()
	if err != nil {
		return result, errors.Wrap(err, "getting schema")
	}

	// deal with indexes
	indexes := s.Indexes()
	var index *pilosa.Index
	if b.Index == "" {
		if len(indexes) == 0 {
			return result, errors.New("no indexes in Pilosa, aborting.")
		}
		for name, idx := range indexes {
			b.Index = name
			index = idx
			break
		}
	} else {
		var ok bool
		index, ok = indexes[b.Index]
		if !ok {
			return result, errors.Errorf("index '%s' not found in schema.", b.Index)
		}
	}

	// we have an index, deal with fields
	var fields []*pilosa.Field
	fieldsMap := index.Fields()
	if len(b.Fields) == 0 {
		fields = make([]*pilosa.Field, 0, len(fieldsMap))
		for _, fld := range fieldsMap {
			fields = append(fields, fld)
		}
	} else {
		fields = make([]*pilosa.Field, 0, len(b.Fields))
		for _, name := range b.Fields {
			fld, ok := fieldsMap[name]
			if !ok {
				return result, errors.Errorf("field '%s' not found in index '%s'.", name, index.Name())
			}
			fields = append(fields, fld)
		}
	}
	if len(fields) == 0 {
		return result, errors.Errorf("no fields to query in index '%s'", b.Index)
	}

	queries := make([]func(...*pilosa.PQLRowQuery) *pilosa.PQLRowQuery, 0)
	if b.Intersect {
		queries = append(queries, index.Intersect)
	}
	if b.Difference {
		queries = append(queries, index.Difference)
	}
	if b.Union {
		queries = append(queries, index.Union)
	}
	if b.Xor {
		queries = append(queries, index.Xor)
	}

	// TODO: Figure out set of rows to use for each field. For now, just apply MaxRowID to all fields.

	start := time.Now()
	eg := errgroup.Group{}
	stats := make([]*NumStats, b.Concurrency)
	for i := 0; i < b.Concurrency; i++ {
		i := i
		stats[i] = NewNumStats()
		eg.Go(func() error {
			return b.runQueries(client, index, fields, queries, i, stats[i])
		})
	}
	err = eg.Wait()
	if err == nil {
		for i := 1; i < len(stats); i++ {
			stats[0].Combine(stats[i])
		}
		result.Extra["countstats"] = stats[0]
		duration := time.Since(start)
		seconds := float64(duration) / 1000000000
		result.Extra["tps"] = float64(b.Iterations*b.Concurrency) / seconds
	}
	return result, err
}

func (b *TPSBenchmark) runQueries(client *pilosa.Client, index *pilosa.Index, fields []*pilosa.Field, queries []func(...*pilosa.PQLRowQuery) *pilosa.PQLRowQuery, seed int, stats *NumStats) error {
	r := rand.New(rand.NewSource(int64(seed)))
	for i := 0; i < b.Iterations; i++ {
		f1 := fields[r.Intn(len(fields))]
		f2 := fields[r.Intn(len(fields))]

		r1 := r.Int63n(b.MaxRowID) + b.MinRowID
		r2 := r.Int63n(b.MaxRowID) + b.MinRowID

		q := queries[r.Intn(len(queries))]

		cntq := index.Count(q(f1.Row(r1), f2.Row(r2)))
		resp, err := client.Query(cntq)
		if err != nil {
			return errors.Wrap(err, "performing query")
		}
		if !resp.Success {
			return errors.Errorf("unsuccessful query: %s", resp.ErrorMessage)
		}
		stats.Add(int64(resp.Result().Count()))
	}
	return nil
}
