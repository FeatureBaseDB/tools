package bench

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/pilosa/pilosa/pql"
)

// NewSliceHeight creates a new slice height benchmark with stdin/out/err
// initialized.
func NewSliceHeight(stdin io.Reader, stdout, stderr io.Writer) *SliceHeight {
	return &SliceHeight{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
	}
}

// SliceHeight benchmark tests the effect of an increasing number of rows in
// a single slice on query time.
type SliceHeight struct {
	MaxTime time.Duration `json:"max-time"`
	hosts   []string

	Name           string `json:"name"`
	BaseIterations int64  `json:"base-iterations"`
	Seed           int64  `json:"seed"`
	Index          string `json:"index"`
	Frame          string `json:"frame"`

	Stdin  io.Reader `json:"-"`
	Stdout io.Writer `json:"-"`
	Stderr io.Writer `json:"-"`
}

// Init sets up the slice height benchmark.
func (b *SliceHeight) Init(hosts []string, agentNum int) error {
	b.Name = "slice-height"
	b.hosts = hosts

	err := initIndex(b.hosts[0], b.Index, b.Frame)
	if err != nil {
		return err
	}
	return nil
}

// Run runs the SliceHeight benchmark
func (b *SliceHeight) Run(ctx context.Context) map[string]interface{} {
	results := make(map[string]interface{})

	imp := &Import{
		MaxRowID:     100,
		MaxColumnID:  1048576, // must match pilosa.SliceWidth
		Iterations:   b.BaseIterations,
		Index:        b.Index,
		Frame:        b.Frame,
		Distribution: "exponential",
		BufferSize:   1000000,
	}

	start := time.Now()

	for i := 0; i > -1; i++ {
		iresults := make(map[string]interface{})
		results["iteration"+strconv.Itoa(i)] = iresults

		err := imp.Init(b.hosts, 0)
		if err != nil {
			iresults["error"] = fmt.Sprintf("error initializing importer, round %d, err: %v", i, err)
			break
		}
		importStart := time.Now()
		importRes := imp.Run(ctx)
		importRes["duration"] = time.Since(importStart)
		iresults["import"] = importRes

		qstart := time.Now()
		q := &pql.Call{
			Name: "TopN",
			Args: map[string]interface{}{
				"frame": b.Frame,
				"n":     50,
			},
		}
		res, err := imp.HasClient.ExecuteQuery(ctx, b.Index, q.String())
		if err != nil {
			iresults["query_error"] = err.Error()
		} else {
			qdur := time.Now().Sub(qstart)
			iresults["query_duration"] = qdur
			iresults["query_results"] = res
		}
		imp.BaseRowID = imp.MaxRowID
		imp.MaxRowID = imp.MaxRowID * 10
		imp.Iterations *= 10

		if time.Now().Sub(start) > b.MaxTime {
			break
		}
	}

	return results
}
