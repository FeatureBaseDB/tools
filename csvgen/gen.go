package csvgen

import (
	"bufio"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"

	"github.com/pkg/errors"
)

type Main struct {
	Filename string
	RunsMin  int
	RunsMax  int
	NMin     int
	NMax     int
	Rows     uint64
	Cols     uint64

	Stdin  io.Reader `json:"-"`
	Stdout io.Writer `json:"-"`
	Stderr io.Writer `json:"-"`
}

func (m *Main) Run() error {
	f, err := os.Create(m.Filename)
	if err != nil {
		return errors.Wrap(err, "opening output file")
	}
	defer f.Close()

	bw := bufio.NewWriter(f)
	defer bw.Flush()

	iter := &Iterator{
		RunsMax: m.RunsMax,
		RunsMin: m.RunsMin,
		NMin:    m.NMin,
		NMax:    m.NMax,
		Rows:    m.Rows,
		Cols:    m.Cols,
	}

	iter.Generate(bw)
	return nil
}

type Iterator struct {
	RunsMin int
	RunsMax int
	NMin    int
	NMax    int
	Rows    uint64
	Cols    uint64
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (iter *Iterator) Generate(w io.Writer) {
	contsPerRow := iter.Cols / 65536
	if iter.Cols%65536 > 0 {
		contsPerRow += 1
	}

	var r uint64
	for r = 0; r < iter.Rows; r++ {
		var c uint64
		for c = 0; c < contsPerRow; c++ {
			numRuns := rand.Intn(iter.RunsMax-iter.RunsMin) + iter.RunsMin
			if numRuns > iter.NMax {
				numRuns = iter.NMax - 1
			}
			numN := rand.Intn(iter.NMax-iter.NMin) + iter.NMin
			if numN < numRuns {
				if numRuns > iter.NMax {
					numRuns = numN
				} else {
					numN = numRuns
				}
			}
			fmt.Fprintf(os.Stderr, "gen container: N: %d, Runs: %d\n", numN, numRuns)
			cont := randomContainer(numN, numRuns)
			for _, col := range cont.Bits() {
				fmt.Fprintf(w, "%v,%v\n", r, (col + (c * 65536)))
			}
		}
	}
}

type interval16 struct {
	start uint16
	last  uint16
}

type container struct {
	runs []interval16
	n    int
}

func (c *container) Bits() []uint64 {
	ret := make([]uint64, 0, c.n)
	for _, run := range c.runs {
		for i := uint64(run.start); i <= uint64(run.last); i++ {
			ret = append(ret, i)
		}
	}
	return ret
}

func randomContainer(N, Nruns int) *container {
	// 1. generate x = [Nruns numbers summing to N]
	// 2. generate y = [Nruns numbers summing to 65536-N]
	// 3. runs = [x0, x0+y0], [x0+y0+x1, x0+y0+x1+y1], ...
	// Then N = (x0+y0 - x0 + 1) + (x0+y0+x1+y1 - x0+y0+x1 + 1) + ...
	//        = y0+1 + y1+1
	// Note: this works by creating Nruns 0-runs interleaved with Nruns 1-runs.
	// This means the first 1-run never starts at 0, and the last 1-run always ends at 65535.
	// preferably, all four options would be possible, with the appropriate probabilities.
	// optionally: twiddle things so we get a mix of containers with bits at 0 or 65535 or not

	c := &container{n: N}
	c.runs = make([]interval16, Nruns)

	set_lengths := randomPartition(N, Nruns, 0, 1)
	clear_lengths := randomPartition(65536-N, Nruns, 1, 0)
	var start, last uint16
	for i := 0; i < Nruns; i++ {
		// TODO check off-by-one issues here
		start = last + uint16(clear_lengths[i])
		last = start + uint16(set_lengths[i])
		c.runs[i] = interval16{start: start, last: last}
	}
	return c
}

// randomPartition generates a slice of positive
// integers of length `num`, with the given `sum`.
func randomPartition(sum, num int, first0, last0 int) []int {
	// Better performance may be possible for small values of num,
	// by using something faster than rand.Perm.
	// the first0 and last0 options allow for including a 0 element in the returned slice.
	// this is a way to allow the possibility of runs starting and ending at any point, when
	// used by randomContainer

	// first0=1 means first element may be 0
	// last0=1 means last element may be 0
	// first0 last0 start end
	// 0      0     1     sum-1
	// 0      1     1     sum
	// 1      0     0     sum-1
	// 1      1     0     sum

	// For a given (sum, num), the number of possible results is ...

	// Generate distinct ints in [start, end].
	permN := sum - 1 + first0 + last0
	if permN < num {
		permN = num
	}
	vals := rand.Perm(permN)[0 : num-1]
	if first0 == 0 {
		for i := 0; i < num-1; i++ {
			vals[i]++
		}
	}

	// Append 0 and sum, then sort
	vals = append(vals, 0)
	vals = append(vals, sum)
	sort.Ints(vals)

	// Now we have an increasing list of ints like [0 a1 a2 ... an sum],
	// where n = num-1, so the length of the list is num+1.
	// return the length-num diff list.
	deltas := make([]int, num)
	for n := 0; n < num; n++ {
		deltas[n] = vals[n+1] - vals[n]
	}
	return deltas
}

// type Bit struct {
// 	Row    uint64
// 	Column uint64
// }

// type BitIterator interface {
// 	Next() (Bit, error)
// }

// func Generate(w io.Writer, bi BitIterator) error {
// 	bit, err := bi.Next()
// 	for ; err == nil; bit, err = bi.Next() {
// 		fmt.Fprintf(w, "%v,%v\n", bit.Row, bit.Column)
// 	}
// 	if err == io.EOF {
// 		return nil
// 	}
// 	return err
// }
