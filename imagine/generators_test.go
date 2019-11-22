package imagine

import (
	"fmt"
	"io"
	"testing"
	"time"

	gopilosa "github.com/pilosa/go-pilosa"
	pilosa "github.com/pilosa/go-pilosa"
)

func testSequenceGenerator(s sequenceGenerator, min int64, max int64, total int64) error {
	seen := make(map[int64]struct{})
	var done bool
	var value int64

	for !done {
		value, done = s.Next()
		if _, ok := seen[value]; ok {
			return fmt.Errorf("generator produced %d more than once", value)
		}
		if value < min || value > max {
			return fmt.Errorf("generator produced value %d, out of range %d..%d", value, min, max)
		}
		seen[value] = struct{}{}
	}
	if int64(len(seen)) != total {
		return fmt.Errorf("generator produced %d values from %d..%d, expecting %d", len(seen), min, max, total)
	}
	return nil
}

func testValueGenerator(v valueGenerator, min int64, max int64, total int64) error {
	seen := make(map[int64]struct{})
	var value int64

	for i := int64(0); i < (max - min); i++ {
		value = v.Nth(i)
		if _, ok := seen[value]; ok {
			return fmt.Errorf("generator produced %d more than once", value)
		}
		if value < min || value > max {
			return fmt.Errorf("generator produced value %d, out of range %d..%d", value, min, max)
		}
		seen[value] = struct{}{}
	}
	if int64(len(seen)) != total {
		return fmt.Errorf("generator produced %d values from %d..%d, expecting %d", len(seen), min, max, total)
	}
	return nil
}

func Test_Generators(t *testing.T) {
	inc := newIncrementGenerator(-3, 5)
	testSequenceGenerator(inc, -3, 5, 9)
	inc2, err := newPermutedGenerator(-3, 5, 7, 0, 0, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	testSequenceGenerator(inc2, -3, 5, 7)
	lin, err := newLinearValueGenerator(-3, 5, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	testValueGenerator(lin, -3, 5, 9)
	perm, err := newPermutedValueGenerator(lin, -3, 5, 9)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	testValueGenerator(perm, -3, 5, 9)
}

func float64p(v float64) *float64 {
	return &v
}
func uint64p(v uint64) *uint64 {
	return &v
}
func int64p(v int64) *int64 {
	return &v
}
func durationp(v duration) *duration {
	return &v
}

func TestFieldMin(t *testing.T) {
	startTime := time.Date(2000, time.Month(1), 2, 3, 4, 5, 6, time.UTC)
	dur := time.Hour * 120
	spec := &taskSpec{
		FieldSpec: &fieldSpec{
			Type:         fieldTypeTime,
			Min:          10,
			Max:          12,
			Chance:       float64p(1.0),
			DensityScale: uint64p(2097152),
			Density:      1.0,
		},
		ColumnOrder:    valueOrderLinear,
		DimensionOrder: dimensionOrderRow,
		Columns:        uint64p(10),
		RowOrder:       valueOrderLinear,
		Seed:           int64p(0),
		Stamp:          stampTypeIncreasing,
		StampStart:     &startTime,
		StampRange:     durationp(duration(dur)),
	}

	updateChan := make(chan taskUpdate, 10)
	go func() {
		for _, ok := <-updateChan; ok; {
		}
	}()
	sg, err := newSetGenerator(spec, updateChan, "updateid")
	if err != nil {
		t.Fatalf("getting new set generator: %v", err)
	}

	r, err := sg.NextRecord()
	if err != nil {
		t.Fatalf("Error in iterator: %v", err)
	}
	col, ok := r.(gopilosa.Column)
	if !ok {
		t.Fatalf("%v not a Column", r)
	}
	if col.RowID != 10 {
		t.Fatalf("field.Min not respected, got row %d, expected 10", col.RowID)
	}

}

func TestNewSetGenerator(t *testing.T) {
	startTime := time.Date(2000, time.Month(1), 2, 3, 4, 5, 6, time.UTC)
	dur := time.Hour * 120
	spec := &taskSpec{
		FieldSpec: &fieldSpec{
			Type:         fieldTypeTime,
			Max:          1,
			Chance:       float64p(1.0),
			DensityScale: uint64p(2097152),
			Density:      1.0,
		},
		ColumnOrder:    valueOrderLinear,
		DimensionOrder: dimensionOrderRow,
		Columns:        uint64p(10),
		RowOrder:       valueOrderLinear,
		Seed:           int64p(0),
		Stamp:          stampTypeIncreasing,
		StampStart:     &startTime,
		StampRange:     durationp(duration(dur)),
	}

	updateChan := make(chan taskUpdate, 10)
	go func() {
		for _, ok := <-updateChan; ok; {
		}
	}()
	sg, err := newSetGenerator(spec, updateChan, "updateid")
	if err != nil {
		t.Fatalf("getting new set generator: %v", err)
	}
	lastT := int64(0)
	i := -1
	endTime := startTime.Add(dur)
	for r, err := sg.NextRecord(); err != io.EOF; r, err = sg.NextRecord() {
		if err != nil {
			t.Fatalf("Error in iterator: %v", err)
		}
		col, ok := r.(gopilosa.Column)
		if !ok {
			t.Fatalf("%v not a Column", r)
		}
		i++
		if col.RowID != 0 {
			t.Fatalf("unexpected row at record %d: %v", i, col)
		}
		if int(col.ColumnID) != i {
			t.Fatalf("unexpected col: exp: %d got %d", i, col.ColumnID)
		}
		if col.Timestamp <= lastT {
			t.Fatalf("unexpected... timestamp did not increase: last: %d this: %v", lastT, col)
		}
		if lastT >= col.Timestamp {
			t.Fatalf("time stamp did not increase, last: %d, this: %d", lastT, col.Timestamp)
		}
		lastT = col.Timestamp
		tim := time.Unix(0, col.Timestamp)
		if tim.Before(startTime) {
			t.Fatalf("got a time before start time: %v", tim)
		}
		if tim.After(endTime) {
			t.Fatalf("got a time after start+duration: %v", tim)
		}
	}
	if endTime.Sub(time.Unix(0, lastT)) > dur/2 {
		t.Fatalf("less than half the duration was used - lastT: %v", lastT)
	}

	close(updateChan)
}

func TestMutexGen(t *testing.T) {
	spec := &taskSpec{
		FieldSpec: &fieldSpec{
			Type:         fieldTypeMutex,
			Max:          2,
			Chance:       float64p(1.0),
			DensityScale: uint64p(2097152),
			Density:      0.9,
			ValueRule:    densityTypeZipf,
			Cache:        cacheTypeLRU,
			ZipfS:        1.1,
			ZipfV:        1,
		},
		ColumnOrder:    valueOrderLinear,
		DimensionOrder: dimensionOrderRow,
		Columns:        uint64p(10),
		RowOrder:       valueOrderLinear,
		Seed:           int64p(0),
	}

	updateChan := make(chan taskUpdate, 10)
	go func() {
		for _, ok := <-updateChan; ok; {
		}
	}()
	sg, err := newMutexGenerator(spec, updateChan, "updateid")
	if err != nil {
		t.Fatalf("getting new set generator: %v", err)
	}

	done := make(chan error)
	go func() {
		for _, err := sg.NextRecord(); err != io.EOF; _, err = sg.NextRecord() {
			if err != nil {
				done <- err
			}
		}
		close(done)
	}()

	select {
	case err = <-done:
		if err != nil {
			t.Fatalf("error in iterator: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("mutex generator hanging")
	}

}

func testOnePatternBits(t *testing.T, p Pattern, name string, ts *taskSpec, expected []pilosa.Column) {
	valGen, err := p.BitGenerator(name, ts, nil, "foo")
	if err != nil {
		t.Fatalf("error creating %s generator: %v", name, err)
	}
	rec, err := valGen.NextRecord()
	count := 0
	for err == nil {
		col, ok := rec.(pilosa.Column)
		if !ok {
			t.Fatalf("%s got unexpected record type %T", name, rec)
		}
		if count >= len(expected) {
			t.Fatalf("extra result: %s got %#v after %d expected results",
				name, col, count)
		}
		if col.ColumnID != expected[count].ColumnID || col.RowID != expected[count].RowID {
			t.Fatalf("expected %s result %d to be %#v, got %#v",
				name, count, expected[count], col)
		}
		count++
		rec, err = valGen.NextRecord()
	}
	if count < len(expected) {
		t.Fatalf("%s early return, %d/%d values", name, count, len(expected))
	}
	if err != io.EOF {
		t.Fatalf("%s expected io.EOF, got %v", name, err)
	}
}

func testOnePatternValues(t *testing.T, p Pattern, name string, ts *taskSpec, expected []pilosa.FieldValue) {
	valGen, err := p.DigitGenerator(name, ts, nil, "foo")
	if err != nil {
		t.Fatalf("error creating %s generator: %v", name, err)
	}
	rec, err := valGen.NextRecord()
	count := 0
	for err == nil {
		col, ok := rec.(pilosa.FieldValue)
		if !ok {
			t.Fatalf("%s got unexpected record type %T", name, rec)
		}
		if count >= len(expected) {
			t.Fatalf("extra result: %s got %#v after %d expected results",
				name, col, count)
		}
		if col.ColumnID != expected[count].ColumnID || col.Value != expected[count].Value {
			t.Fatalf("expected %s result %d to be %#v, got %#v",
				name, count, expected[count], col)
		}
		count++
		rec, err = valGen.NextRecord()
	}
	if count < len(expected) {
		t.Fatalf("%s early return, %d/%d values", name, count, len(expected))
	}
	if err != io.EOF {
		t.Fatalf("%s expected io.EOF, got %v", name, err)
	}
}

type triangleSpecExpected struct {
	n, exp, col    int64
	expectedBits   map[string][]pilosa.Column
	expectedValues []pilosa.FieldValue
}

func TestPatternGen(t *testing.T) {
	testCases := []triangleSpecExpected{
		{
			n:   3,
			exp: 0,
			col: 8,
			expectedBits: map[string][]pilosa.Column{
				"equal": []pilosa.Column{
					{ColumnID: 0, RowID: 0},
					{ColumnID: 1, RowID: 0},
					{ColumnID: 3, RowID: 0},
					{ColumnID: 6, RowID: 0},
					{ColumnID: 7, RowID: 0},
					{ColumnID: 2, RowID: 1},
					{ColumnID: 4, RowID: 1},
					{ColumnID: 5, RowID: 2},
				},
				"once": []pilosa.Column{
					{ColumnID: 0, RowID: 0},
					{ColumnID: 6, RowID: 0},
					{ColumnID: 2, RowID: 1},
					{ColumnID: 5, RowID: 2},
				},
				"upto": []pilosa.Column{
					{ColumnID: 0, RowID: 0},
					{ColumnID: 1, RowID: 0},
					{ColumnID: 3, RowID: 0},
					{ColumnID: 6, RowID: 0},
					{ColumnID: 7, RowID: 0},
					{ColumnID: 0, RowID: 1},
					{ColumnID: 1, RowID: 1},
					{ColumnID: 2, RowID: 1},
					{ColumnID: 3, RowID: 1},
					{ColumnID: 4, RowID: 1},
					{ColumnID: 6, RowID: 1},
					{ColumnID: 7, RowID: 1},
					{ColumnID: 0, RowID: 2},
					{ColumnID: 1, RowID: 2},
					{ColumnID: 2, RowID: 2},
					{ColumnID: 3, RowID: 2},
					{ColumnID: 4, RowID: 2},
					{ColumnID: 5, RowID: 2},
					{ColumnID: 6, RowID: 2},
					{ColumnID: 7, RowID: 2},
				},
				"over": []pilosa.Column{
					{ColumnID: 0, RowID: 0},
					{ColumnID: 1, RowID: 0},
					{ColumnID: 2, RowID: 0},
					{ColumnID: 3, RowID: 0},
					{ColumnID: 4, RowID: 0},
					{ColumnID: 5, RowID: 0},
					{ColumnID: 6, RowID: 0},
					{ColumnID: 7, RowID: 0},
					{ColumnID: 2, RowID: 1},
					{ColumnID: 4, RowID: 1},
					{ColumnID: 5, RowID: 1},
					{ColumnID: 5, RowID: 2},
				},
			},
			expectedValues: []pilosa.FieldValue{
				{ColumnID: 0, Value: 0},
				{ColumnID: 1, Value: 0},
				{ColumnID: 2, Value: 1},
				{ColumnID: 3, Value: 0},
				{ColumnID: 4, Value: 1},
				{ColumnID: 5, Value: 2},
				{ColumnID: 6, Value: 3},
				{ColumnID: 7, Value: 3},
			},
		},
		{
			n:   3,
			exp: 1,
			col: 16,
			expectedBits: map[string][]pilosa.Column{
				"once": []pilosa.Column{
					{ColumnID: 0, RowID: 0},
					{ColumnID: 1, RowID: 0},
					{ColumnID: 2, RowID: 0},
					{ColumnID: 6, RowID: 1},
					{ColumnID: 7, RowID: 1},
					{ColumnID: 8, RowID: 1},
					{ColumnID: 15, RowID: 2},
				},
			},
			expectedValues: []pilosa.FieldValue{
				{ColumnID: 0, Value: 0},
				{ColumnID: 1, Value: 1},
				{ColumnID: 2, Value: 2},
				{ColumnID: 3, Value: 0},
				{ColumnID: 4, Value: 1},
				{ColumnID: 5, Value: 2},
				{ColumnID: 6, Value: 3},
				{ColumnID: 7, Value: 4},
				{ColumnID: 8, Value: 5},
				{ColumnID: 9, Value: 0},
				{ColumnID: 10, Value: 1},
				{ColumnID: 11, Value: 2},
				{ColumnID: 12, Value: 3},
				{ColumnID: 13, Value: 4},
				{ColumnID: 14, Value: 5},
				{ColumnID: 15, Value: 6},
			},
		},
	}
	for _, testCase := range testCases {
		p, err := newTrianglePattern(testCase.n, testCase.exp, 0)
		if err != nil {
			t.Fatalf("generating trianglePattern: %v", err)
		}
		colVal := uint64(testCase.col)
		ts := taskSpec{Columns: &colVal}
		for name, expected := range testCase.expectedBits {
			testOnePatternBits(t, p, name, &ts, expected)
		}
		if testCase.expectedValues != nil {
			testOnePatternValues(t, p, "value", &ts, testCase.expectedValues)
		}
	}
}
