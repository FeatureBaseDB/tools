package imagine

import (
	"fmt"
	"io"
	"testing"
	"time"

	gopilosa "github.com/pilosa/go-pilosa"
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
	inc2, err := newPermutedGenerator(-3, 5, 7, 0, 1)
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

	done := make(chan struct{})
	go func() {
		for _, err := sg.NextRecord(); err != io.EOF; _, err = sg.NextRecord() {
			if err != nil {
				t.Fatalf("Error in iterator: %v", err)
			}
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("mutex generator hanging")
	}

}
