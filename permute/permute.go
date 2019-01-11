package permute

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"math/bits"
	"math/rand"
)

// PermutationGenerator provides a way to pass integer IDs through a permutation
// map that is pseudorandom but repeatable. This could be done with rand.Perm,
// but that would require storing a [Iterations]int64 array, which we want to avoid
// for large values of Iterations.
//
// Not actually cryptographically secure.
type PermutationGenerator struct {
	aes          cipher.Block
	max          int64
	counter      int64
	rounds       int
	hIn, hOut    []byte
	hits, values int64
}

// NewPermutationGenerator creates a PermutationGenerator which will
// generate values in [0,m), given the provided seed.
func NewPermutationGenerator(max int64, seed int64) (*PermutationGenerator, error) {
	if max < 1 {
		return nil, errors.New("period must be positive")
	}
	// number of rounds to get "good" results is roughly 6 log N. but that gets
	// pretty slow, so we use fewer.
	bits := 64 - bits.LeadingZeros64(uint64(max))
	p := PermutationGenerator{max: max, rounds: 6 * bits, counter: 0}

	src := rand.NewSource(seed)
	rnd := rand.New(src)
	aesKey := make([]byte, 16)
	n, err := rnd.Read(aesKey)
	if n != 16 || err != nil {
		return nil, err
	}
	p.aes, err = aes.NewCipher(aesKey)
	if err != nil {
		return nil, err
	}
	p.hIn = make([]byte, p.aes.BlockSize())
	p.hOut = make([]byte, p.aes.BlockSize())
	return &p, nil
}

// Next generates the next value from the permutation generator.
func (p *PermutationGenerator) Next() (ret int64) {
	return p.nextValue()
}

// Nth generates the Nth value from the permutation generator. For instance,
// given a new permutation generator, calling Next() once produces the same
// value you'd get from calling Nth(0). Calling Next() twice produces the same
// value as calling Nth(1). The Nth call changes the location of the permutation
// generator within its sequence.
func (p *PermutationGenerator) Nth(n int64) (ret int64) {
	p.counter = n
	ret = p.nextValue()
	p.values++
	return ret
}

func (p *PermutationGenerator) nextValue() int64 {
	p.counter++
	p.counter = int64(uint64(p.counter) % uint64(p.max))
	x := uint64(p.counter)
	for i := uint64(1); i <= uint64(p.rounds); i++ {
		binary.LittleEndian.PutUint64(p.hIn, i)
		p.aes.Encrypt(p.hOut, p.hIn)
		crypt := binary.LittleEndian.Uint64(p.hOut)
		kSubI := crypt % uint64(p.max)
		xPrime := (kSubI + uint64(p.max) - x) % uint64(p.max)
		xCaret := x
		if xPrime > xCaret {
			xCaret = xPrime
		}
		binary.LittleEndian.PutUint64(p.hIn, uint64(p.max)*i+xCaret)
		p.aes.Encrypt(p.hOut, p.hIn)
		crypt = binary.LittleEndian.Uint64(p.hOut)
		fSubI := crypt >> 63
		if fSubI == 1 {
			x = xPrime
		}
	}
	p.hits++
	return int64(x)
}

// Hits tells you how many values you've gotten, and how many values were
// generated to get them. Worst-case rounding would be that, for a range of
// [0,5), the algorithm in use needs to cycle over 16 values -- it rounds up
// to 2^3, and then to 2^4 because it needs the next even power of 2.
func (p *PermutationGenerator) Hits() (values int64, hits int64) {
	return p.values, p.hits
}
