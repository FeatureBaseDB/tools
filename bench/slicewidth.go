package bench

import (
	"context"
	"io"
	"strconv"
	"fmt"
	"github.com/pilosa/pilosa/pql"
	"time"
)

// NewSliceHeight creates a new slice height benchmark with stdin/out/err
// initialized.
func NewSliceWidth(stdin io.Reader, stdout, stderr io.Writer) *SliceWidth {
	return &SliceWidth{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
	}
}

// SliceHeight benchmark tests the effect of an increasing number of rows in
// a single slice on query time.
type SliceWidth struct {
	HasClient
	MaxTime time.Duration `json:"max-time"`
	hosts   []string

	Name           string  `json:"name"`
	BaseIterations int64   `json:"base-iterations"`
	Seed           int64   `json:"seed"`
	Index          string  `json:"index"`
	Frame          string  `json:"frame"`
	BitDensity     float64 `json:"bit-density"`
	MaxColumnID    int64   `json:"max-column-id"`
	SliceCount     int64   `json:"slice-count"`

	Stdin  io.Reader `json:"-"`
	Stdout io.Writer `json:"-"`
	Stderr io.Writer `json:"-"`
}

// Init sets up the slice height benchmark.
func (b *SliceWidth) Init(hosts []string, agentNum int) error {
	b.Name = "slice-width"
	b.hosts = hosts

	err := initIndex(b.hosts[0], b.Index, b.Frame)
	if err != nil {
		return err
	}
	return b.HasClient.Init(hosts, agentNum)
}

// Run runs the SliceWidth benchmark
func (b *SliceWidth) Run(ctx context.Context) map[string]interface{} {
	results := make(map[string]interface{})
	defer b.CallQuery(ctx, results)
	bitDensity := b.BitDensity
	iteration := b.BaseIterations
	sliceCount := b.SliceCount
	if bitDensity > 0 {
		iteration = int64(float64(b.MaxColumnID) * bitDensity)
	}

	// uniformly import to different slices
	for i :=int64(1); i<=sliceCount; i++ {
		iresults := make(map[string]interface{})
		results["iteration"+strconv.Itoa(int(i))] = iresults
		maxColumnID := b.MaxColumnID * i
		baseColumnID := b.MaxColumnID * (i-1)
		imp := &Import{
			MaxRowID:     100,
			BaseColumnID: baseColumnID,
			MaxColumnID:  maxColumnID,
			Iterations:   iteration,
			Index:        b.Index,
			Frame:        b.Frame,
			Distribution: "uniform",
			BufferSize:   1000000,
		}

		err := imp.Init(b.hosts, 0)
		if err != nil {
			iresults["error"] = fmt.Sprintf("error initializing importer, err: %v", err)
		}

		importStart := time.Now()
		importRes := imp.Run(ctx)
		importRes["duration"] = time.Since(importStart)
		iresults["import"] = importRes

	}
	return results
}

func (b *SliceWidth) CallQuery(ctx context.Context, results map[string]interface{}) map[string]interface{} {
	q := &pql.Call{
		Name: "TopN",
		Args: map[string]interface{}{
			"frame": b.Frame,
			"n":     10000,
		},
	}
	iresults := make(map[string]interface{})
	qstart := time.Now()
	res, err := b.ExecuteQuery(ctx, b.Index, q.String())
	if err != nil {
		iresults["query_error"] = err.Error()
	} else {
		qdur := time.Now().Sub(qstart)
		iresults["query_duration"] = qdur
		iresults["query_results"] = res
	}

	results["output"] = iresults
	return results
}
