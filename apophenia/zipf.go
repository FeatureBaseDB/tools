package apophenia

import (
	"math"
)

// Zipf produces a series of values following a Zipf distribution.
// It is initialized with values q, v, and max, and produces values
// in the range [0,max) such that the probability of a value k is
// proportional to (v+k) ** -q. v must be >= 1, q must be > 1.
//
// This is based on the same paper used for the golang stdlib Zipf
// distribution:
//
// "Rejection-Inversion to Generate Variates
// from Monotone Discrete Distributions"
// W.Hormann, G.Derflinger [1996]
// http://eeyore.wu-wien.ac.at/papers/96-04-04.wh-der.ps.gz
//
// This implementation differs from stdlib's in that it is seekable; you
// can get the Nth value in a theoretical series of results in constant
// time, without having to generate the whole series linearly.
type Zipf struct {
	src                  Sequence
	seed                 uint32
	q                    float64
	v                    float64
	max                  float64
	oneMinusQ            float64
	oneOverOneMinusQ     float64
	h                    func(float64) float64
	hInv                 func(float64) float64
	hImaxOneHalf         float64
	hX0MinusHImaxOneHalf float64 // hX0 is only ever used as hX0 - h(i[max] + 1/2)
	s                    float64
	idx                  uint64
}

// NewZipf returns a new Zipf object with the specified q, v, and
// max, and with its random source seeded in some way by seed.
// The sequence of values returned is consistent for a given set
// of inputs. The seed parameter can select one of multiple sub-sequences
// of the given sequence.
func NewZipf(q float64, v float64, max uint64, seed uint32, src Sequence) (z *Zipf, err error) {
	oneMinusQ := 1 - q
	oneOverOneMinusQ := 1 / (1 - q)
	z = &Zipf{
		q:                q,
		v:                v,
		max:              float64(max),
		seed:             seed,
		oneMinusQ:        oneMinusQ,
		oneOverOneMinusQ: oneOverOneMinusQ,
	}
	z.h = func(x float64) float64 {
		return math.Exp((1-q)*math.Log(v+x)) * oneOverOneMinusQ
	}
	z.hInv = func(x float64) float64 {
		return -v + math.Exp(oneOverOneMinusQ*math.Log(oneMinusQ*x))
	}
	hX0 := z.h(0.5) - math.Exp(math.Log(v)*-q)
	z.hImaxOneHalf = z.h(z.max + 0.5)
	z.hX0MinusHImaxOneHalf = hX0 - z.hImaxOneHalf
	z.s = 1 - z.hInv(z.h(1.5)-math.Exp(math.Log(v+1)*-q))
	z.src = src
	if err != nil {
		return nil, err
	}
	return z, nil
}

// Nth returns the Nth value from the sequence associated with the
// given Zipf. For a given set of input values (q, v, max, and seed),
// and a given index, the same value is returned.
func (z *Zipf) Nth(index uint64) uint64 {
	z.idx = index
	offset := OffsetFor(SequenceZipfU, z.seed, 0, index)
	for {
		bits := z.src.BitsAt(offset)
		uInt := bits.Lo
		u := float64(uInt&(1<<53-1)) / (1 << 53)
		u = z.hImaxOneHalf + u*z.hX0MinusHImaxOneHalf
		x := z.hInv(u)
		k := math.Floor(x + 0.5)
		if k-x <= z.s {
			return uint64(k)
		}
		if u >= z.h(k+0.5)-math.Exp(-math.Log(z.v+k)*z.q) {
			return uint64(k)
		}
		// the low-order 24 bits of the high-order 64-bit word
		// are the "iteration", which started as zero. Assuming we
		// don't need more than ~16.7M values, we're good. The expected
		// average is about 1.1.
		offset.Hi++
	}
}

// Next returns the "next" value -- the one after the last one requested, or
// value 1 if none have been requested before.
func (z *Zipf) Next() uint64 {
	return z.Nth(z.idx + 1)
}
