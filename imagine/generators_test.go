package main

import (
	"fmt"
	"testing"
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
	perm, err := permuteValueGenerator(lin, -3, 5, 9)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	testValueGenerator(perm, -3, 5, 9)
}
