package apophenia

import (
	"fmt"
	"testing"
)

func permutationGeneratorOrBust(period int64, seed int64, expectedErr string, tb testing.TB) *PermutationGenerator {
	seq := NewSequence(seed)
	p, err := NewPermutationGenerator(period, 0, seq)
	if err != nil {
		if expectedErr == "" || expectedErr != err.Error() {
			tb.Fatalf("unexpected error creating permutation generator: %s", err)
		}
		return p
	}
	if p == nil {
		tb.Fatalf("unexpected nil permutation generator without error")
	}
	return p
}

func Test_PermuteCycle(t *testing.T) {
	sizes := []int64{8, 23, 64, 10000}
	for _, size := range sizes {
		p := permutationGeneratorOrBust(size, 0, "", t)
		seen := make(map[int64]struct{}, size)
		for i := int64(0); i < size; i++ {
			n := p.Next()
			if _, ok := seen[n]; ok {
				list := make([]int64, len(seen))
				j := 0
				for k := range seen {
					list[j] = k
					j++
				}
				t.Fatalf("size %d: got duplicate entry %d in %v", size, n, list)
			}
			seen[n] = struct{}{}
		}
	}
}

func TestPermuteSeed(t *testing.T) {
	size := int64(129)
	seeds := int64(8)
	p := make([]*PermutationGenerator, seeds)
	seen := make([]map[int64]struct{}, seeds)
	for s := int64(0); s < seeds; s++ {
		p[s] = permutationGeneratorOrBust(size, s, "", t)
		seen[s] = make(map[int64]struct{}, size)
	}
	matches := int64(0)
	v := make([]int64, seeds)
	for i := int64(0); i < size; i++ {
		for s := int64(0); s < seeds; s++ {
			v[s] = p[s].Next()
			if _, ok := seen[s][v[s]]; ok {
				t.Fatalf("duplicate entry (size %d, seed %d, entry %d): %d",
					size, s, i, v[s])
			}
			seen[s][v[s]] = struct{}{}
		}
		for s := int64(1); s < seeds; s++ {
			if v[s] == v[s-1] {
				matches++
			}
		}
	}
	// assuming number of outcomes is more than about 16, matches are pretty rare if nothing
	// is wrong.
	if (matches * 8) > (size * seeds) {
		t.Fatalf("too many matches: %d values to permute, %d seeds, %d matches seems suspicious.",
			size, seeds, matches)
	} else {
		t.Logf("permuting %d values across %d seeds: %d matches (OK)", size, seeds, matches)
	}
}

func Test_PermuteNth(t *testing.T) {
	size := int64(129)
	seeds := int64(8)
	p := make([][]*PermutationGenerator, seeds)
	seen := make([]map[int64]struct{}, seeds)
	for s := int64(0); s < seeds; s++ {
		p[s] = make([]*PermutationGenerator, 2)
		p[s][0] = permutationGeneratorOrBust(size, s, "", t)
		p[s][1] = permutationGeneratorOrBust(size, s, "", t)
		seen[s] = make(map[int64]struct{}, size)
	}
	matches := int64(0)
	v := make([]int64, seeds)
	for i := int64(0); i < size; i++ {
		for s := int64(0); s < seeds; s++ {
			v[s] = p[s][0].Next()
			if _, ok := seen[s][v[s]]; ok {
				t.Fatalf("duplicate entry (size %d, seed %d, entry %d): %d",
					size, s, i, v[s])
			}
			seen[s][v[s]] = struct{}{}
			vN := p[s][1].Nth(i)
			if vN != v[s] {
				t.Fatalf("Nth entry didn't match Nth call to Next()) (size %d, seed %d, n %d): expected %d, got %d\n",
					size, s, i, v[s], vN)
			}
		}
	}
	// assuming number of outcomes is more than about 16, matches are pretty rare if nothing
	// is wrong.
	if (matches * 8) > (size * seeds) {
		t.Fatalf("too many matches: %d values to permute, %d seeds, %d matches seems suspicious.",
			size, seeds, matches)
	} else {
		t.Logf("permuting %d values across %d seeds: %d matches (OK)", size, seeds, matches)
	}
}

func Benchmark_PermuteCycle(b *testing.B) {
	sizes := []int64{5, 63, 1000000, (1 << 19)}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("Pool%d", size), func(b *testing.B) {
			p := permutationGeneratorOrBust(size, 0, "", b)
			for i := 0; i < b.N; i++ {
				_ = p.Next()
			}
		})
	}
}
