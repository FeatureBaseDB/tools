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

type zipfTestCase struct {
	q, v float64
	seq  Sequence
	exp  string
}

func (z zipfTestCase) String() string {
	return fmt.Sprintf("q: %g, v: %g, seq: %t, expected error: %t",
		z.q, z.v, z.seq != nil, z.exp != "")
}

func Test_InvalidInputs(t *testing.T) {
	seq := NewSequence(0)
	testCases := []zipfTestCase{
		{q: 1, v: 1.1, seq: seq, exp: "need q > 1 (got 1) and v >= 1 (got 1.1) for Zipf distribution"},
		{q: 1.1, v: 0.99, seq: seq, exp: "need q > 1 (got 1.1) and v >= 1 (got 0.99) for Zipf distribution"},
		{q: 1.1, v: 1.1, seq: nil, exp: "need a usable PRNG apophenia.Sequence"},
		{q: math.NaN(), v: 1.1, seq: nil, exp: "q (NaN) and v (1.1) must not be NaN for Zipf distribution"},
		{q: 1.01, v: 2, seq: seq, exp: ""},
	}
	for _, c := range testCases {
		z, err := NewZipf(c.q, c.v, 20, 0, c.seq)
		if c.exp != "" {
			if err == nil {
				t.Errorf("case %v: expected error '%s', got no error", c, c.exp)
			} else if err.Error() != c.exp {
				t.Errorf("case %v: expected error '%s', got error '%s'", c, c.exp, err.Error())
			}
		} else {
			if err != nil {
				t.Errorf("case %v: unexpected error %v", c, err)
			} else if z == nil {
				t.Errorf("case %v: nil Zipf despite no error", c)
			}
		}
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
