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
	aes       cipher.Block
	max       int64
	counter   int64
	rounds    int
	hIn, hOut []byte
	k         []uint64
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
	p.k = make([]uint64, p.rounds)
	for i := uint64(0); i < uint64(p.rounds); i++ {
		binary.LittleEndian.PutUint64(p.hIn, i)
		p.aes.Encrypt(p.hOut, p.hIn)
		crypt := binary.LittleEndian.Uint64(p.hOut)
		p.k[i] = crypt % uint64(p.max)
	}
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
	return ret
}

func (p *PermutationGenerator) nextValue() int64 {
	p.counter++
	p.counter = int64(uint64(p.counter) % uint64(p.max))
	x := uint64(p.counter)
	// a value which can't possibly be the next value we need, so we
	// always hash on the first pass.
	prev := uint64(p.max) + 1
	for i := uint64(0); i < uint64(p.rounds); i++ {
		word, bit := (i/8)%16, byte(1<<(i&7))
		xPrime := (p.k[i] + uint64(p.max) - x) % uint64(p.max)
		xCaret := x
		if xPrime > xCaret {
			xCaret = xPrime
		}
		if xCaret != prev {
			binary.LittleEndian.PutUint64(p.hIn, xCaret)
			p.aes.Encrypt(p.hOut, p.hIn)
			prev = xCaret
		}
		if p.hOut[word]&bit != 0 {
			x = xPrime
		}
	}
	return int64(x)
}
