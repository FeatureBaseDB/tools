package apophenia

import (
	"errors"
	"math/bits"
)

// Weighted provides a generator which produces weighted bits -- bits with
// a specified probability of being set, as opposed to random values weighted
// a given way. Bit density is specified as N/M, with M a (positive)
// power of 2, and N an integer between 0 and M.
//
// The underlying generator can produce 2^128 128-bit values, but the
// iterative process requires log2(densityScale) 128-bit values from the
// source per 128-bit output, so the top 7 bits of the address are used
// as an iteration counter. Thus, Weighted.Bit can produce 2^128 distinct
// values, but if the offset provided to Weighted.Bits is over 2^121, there
// will be overlap between the source values used for those bits, and
// source values used (for different iterations) for other offsets.
type Weighted struct {
	src Sequence
	// internal result cache
	lastValue              Uint128
	lastOffset             Uint128
	lastDensity, lastScale uint64
}

// NewWeighted yields a new Weighted using the given sequence as a source of
// seekable pseudo-random bits.
func NewWeighted(src Sequence) (*Weighted, error) {
	if src == nil {
		return nil, errors.New("new Weighted requires a non-nil source")
	}
	w := Weighted{src: src}
	return &w, nil
}

// Bit returns the single 0-or-1 bit at the specified offset.
func (w *Weighted) Bit(offset Uint128, density uint64, scale uint64) uint64 {
	var bit uint64
	// In order to be able to cache/reuse values, we want to grab a whole
	// set of 128 bits including a given offset, and use the same
	// calculation for all of them. So we mask out the low-order 7 bits
	// of offset, and use them separately. Meanwhile, Bits will
	// always right-shift its column bits by 7, which reduces the
	// space of possible results but means that it produces the same
	// set of bits for any given batch...
	offset.lo, bit = offset.lo&^127, offset.lo&127
	if offset == w.lastOffset && density == w.lastDensity && scale == w.lastScale {
		return w.lastValue.Bit(bit)
	}
	w.lastValue = w.Bits(offset, density, scale)
	w.lastOffset, w.lastDensity, w.lastScale = offset, density, scale
	return w.lastValue.Bit(bit)
}

const weightedIterationMask = (^uint64(0)) >> 7

// Bits returns the 128-bit set of bits including offset. The column portion
// of offset is right-shifted by 7 to match the offset calculations in Bit(),
// above. Thus, you get the same values back for each sequence of 128 consecutive
// offsets.
func (w *Weighted) Bits(offset Uint128, density uint64, scale uint64) (out Uint128) {
	// magic accommodation for choices made elsewhere.
	offset.lo >>= 7
	if density == scale {
		out.Not()
		return out
	}
	if density == 0 {
		return out
	}
	lz := uint(bits.TrailingZeros64(density))
	density >>= lz
	scale >>= lz
	// generate the same results we would have without this hackery
	offset.hi += uint64(lz)
	for scale > 1 {
		next := w.src.BitsAt(offset)
		if density&1 != 0 {
			out.Or(next)
		} else {
			out.And(next)
		}
		density >>= 1
		scale >>= 1
		// iteration is stashed in the bottom 24-bits of an offset
		offset.hi++
	}
	return out
}

// NextBits returns the next batch of bits after the last one retrieved.
func (w *Weighted) NextBits(density, scale uint64) (out Uint128) {
	w.lastOffset.Inc()
	return w.Bits(w.lastOffset, density, scale)
}
