package apophenia

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
)

type testCase struct {
	name string
	s, v float64
	m    uint64
}

var testCases = []testCase{
	{s: 1.01, v: 1, m: 100},
	{s: 2, v: 1, m: 100},
	{s: 1.01, v: 100, m: 1000},
	{s: 2, v: 10000, m: 1000},
}

func (tc testCase) Name() string {
	if tc.name != "" {
		return tc.name
	}
	return fmt.Sprintf("(zipf:s%f,v%f,m%d)", tc.s, tc.v, tc.m)
}

func runZipf(zf func() uint64, values []uint64, n uint64, t *testing.T) {
	for i := uint64(0); i < n; i++ {
		x := zf()
		if x < 0 || x >= uint64(len(values)) {
			t.Fatalf("got out-of-range value %d from zipf function", x)
		}
		values[x]++
	}
}

const runs = 1000000

func Test_CompareWithMath(t *testing.T) {
	failed := false
	for idx, c := range testCases {
		stdlibValues := make([]uint64, c.m+1)
		zipfValues := make([]uint64, c.m+1)
		stdlibZipf := rand.NewZipf(rand.New(rand.NewSource(int64(idx))), c.s, c.v, c.m)
		seq := NewSequence(int64(idx))
		zipfZipf, err := NewZipf(c.s, c.v, c.m, 0, seq)
		if err != nil {
			t.Fatalf("failed to create newZipf: %s", err)
		}
		runZipf(stdlibZipf.Uint64, stdlibValues, runs, t)
		runZipf(zipfZipf.Next, zipfValues, runs, t)
		for i := uint64(0); i < c.m; i++ {
			stdlibP := float64(stdlibValues[i]) / runs
			zipfP := float64(zipfValues[i]) / runs
			diff := math.Abs(stdlibP - zipfP)
			if diff > 0.001 {
				failed = true
				t.Logf("%s: stdlib %d, zipf %d, diff %f [s %f, v %f]",
					c.Name(), stdlibValues[i], zipfValues[i], diff, c.s, c.v)
			}
		}
	}
	if failed {
		t.Fail()
	}
}
