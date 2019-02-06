package apophenia

import (
	"fmt"
	"testing"
)

func Benchmark_WeightedDistribution(b *testing.B) {
	src := NewSequence(0)
	w, err := NewWeighted(src)
	if err != nil {
		b.Fatalf("couldn't make weighted: %v", err)
	}
	scales := []uint64{3, 6, 12, 18, 24, 63}
	for _, scale := range scales {
		off := OffsetFor(SequenceWeighted, 0, 0, 0)
		scaled := uint64(1 << scale)
		b.Run(fmt.Sprintf("Scale%d", scale), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				w.Bits(off, 1, scaled)
				w.Bits(off, scaled/2, scaled)
				w.Bits(off, scaled-1, scaled)
			}
		})
	}

}
