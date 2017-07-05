package bench

import (
	"context"
	"io"
	"strconv"
	"time"

	"github.com/pilosa/pilosa"
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

	Name          string `json:"name"`
	MinBitsPerMap int64  `json:"min-bits-per-map"`
	MaxBitsPerMap int64  `json:"max-bits-per-map"`
	Seed          int64  `json:"seed"`
	Index         string `json:"index"`
	Frame         string `json:"frame"`

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

	imp := NewImport(b.Stdin, b.Stdout, b.Stderr)
	imp.MaxRowID = 100
	imp.MaxColumnID = pilosa.SliceWidth
	imp.MinBitsPerMap = b.MinBitsPerMap
	imp.MaxBitsPerMap = b.MaxBitsPerMap
	imp.Index = b.Index
	imp.Frame = b.Frame

	start := time.Now()

	for i := 0; i > -1; i++ {
		iresults := make(map[string]interface{})
		results["iteration"+strconv.Itoa(i)] = iresults

		genstart := time.Now()
		imp.Init(b.hosts, 0)
		gendur := time.Now().Sub(genstart)
		iresults["csvgen"] = gendur

		iresults["import"] = imp.Run(ctx)

		qstart := time.Now()
		q := &pql.Call{
			Name: "TopN",
			Args: map[string]interface{}{
				"frame": b.Frame,
				"n":     50,
			},
		}
		_, err := imp.Client.ExecuteQuery(ctx, b.Index, q.String(), true)
		if err != nil {
			iresults["query_error"] = err.Error()
		} else {
			qdur := time.Now().Sub(qstart)
			iresults["query"] = qdur
		}
		imp.BaseRowID = imp.MaxRowID
		imp.MaxRowID = imp.MaxRowID * 10

		if time.Now().Sub(start) > b.MaxTime {
			break
		}
	}

	return results
}
