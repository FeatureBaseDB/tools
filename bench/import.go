package bench

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"

	"sort"

	"github.com/pilosa/pilosa/ctl"
)

// NewImport returns an Import Benchmark which pilosa/ctl importer configured.
func NewImport(stdin io.Reader, stdout, stderr io.Writer) *Import {
	return &Import{
		ImportCommand: ctl.NewImportCommand(stdin, stdout, stderr),
	}
}

// Import sets bits with increasing column id and row id.
type Import struct {
	Name           string `json:"name"`
	BaseRowID      int64  `json:"base-row-id"`
	MaxRowID       int64  `json:"max-row-id"`
	BaseColumnID   int64  `json:"base-column-id"`
	MaxColumnID    int64  `json:"max-column-id"`
	RandomRowOrder bool   `json:"random-row-order"`
	MinBitsPerMap  int64  `json:"min-bits-per-map"`
	MaxBitsPerMap  int64  `json:"max-bits-per-map"`
	AgentControls  string `json:"agent-controls"`
	Seed           int64  `json:"seed"`
	numbits        int

	*ctl.ImportCommand
}

// Init generates import data based on the agent num and fields of 'b'.
func (b *Import) Init(hosts []string, agentNum int) error {
	if len(hosts) == 0 {
		return fmt.Errorf("Need at least one host")
	}
	b.Name = "import"
	b.Host = hosts[0]
	// generate csv data
	b.Seed = b.Seed + int64(agentNum)
	switch b.AgentControls {
	case "height":
		numRowIDs := (b.MaxRowID - b.BaseRowID)
		b.BaseRowID = b.BaseRowID + (numRowIDs * int64(agentNum))
		b.MaxRowID = b.BaseRowID + numRowIDs
	case "width":
		numColumnIDs := (b.MaxColumnID - b.BaseColumnID)
		b.BaseColumnID = b.BaseColumnID + (numColumnIDs * int64(agentNum))
		b.MaxColumnID = b.BaseColumnID + numColumnIDs
	case "":
		break
	default:
		return fmt.Errorf("agent-controls: '%v' is not supported", b.AgentControls)
	}
	f, err := ioutil.TempFile("", "")
	if err != nil {
		return err
	}
	// set b.Paths)
	num := GenerateImportCSV(f, b.BaseRowID, b.MaxRowID, b.BaseColumnID, b.MaxColumnID,
		b.MinBitsPerMap, b.MaxBitsPerMap, b.Seed, b.RandomRowOrder)
	b.numbits = num
	// set b.Paths
	b.Paths = []string{f.Name()}

	err = initIndex(b.Host, b.Index, b.Frame)
	if err != nil {
		return err
	}

	return f.Close()
}

// Run runs the Import benchmark
func (b *Import) Run(ctx context.Context) map[string]interface{} {
	results := make(map[string]interface{})
	results["numbits"] = b.numbits
	results["index"] = b.Index
	err := b.ImportCommand.Run(ctx)

	if err != nil {
		results["error"] = err.Error()
	}
	return results
}

// Int64Slice is a sortable slice of 64 bit signed ints
type Int64Slice []int64

func (s Int64Slice) Len() int           { return len(s) }
func (s Int64Slice) Less(i, j int) bool { return s[i] < s[j] }
func (s Int64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// GenerateImportCSV writes a generated csv to 'w' which is in the form pilosa/ctl expects for imports.
func GenerateImportCSV(w io.Writer, baseRowID, maxRowID, baseColumnID, maxColumnID, minBitsPerMap, maxBitsPerMap, seed int64, randomOrder bool) int {
	src := rand.NewSource(seed)
	rng := rand.New(src)

	var rowIDs []int
	if randomOrder {
		rowIDs = rng.Perm(int(maxRowID - baseRowID))
	}
	numrows := 0
	columnIDs := make(Int64Slice, maxBitsPerMap)
	for i := baseRowID; i < maxRowID; i++ {
		var rowID int64
		if randomOrder {
			rowID = int64(rowIDs[i-baseRowID])
		} else {
			rowID = int64(i)
		}

		numBitsToSet := rng.Int63n(maxBitsPerMap-minBitsPerMap) + minBitsPerMap
		numrows += int(numBitsToSet)
		for j := int64(0); j < numBitsToSet; j++ {
			columnIDs[j] = rng.Int63n(maxColumnID-baseColumnID) + baseColumnID
		}
		profIDs := columnIDs[:numBitsToSet]
		if !randomOrder {
			sort.Sort(profIDs)
		}
		for j := int64(0); j < numBitsToSet; j++ {
			fmt.Fprintf(w, "%d,%d\n", rowID, profIDs[j])
		}

	}
	return numrows
}
