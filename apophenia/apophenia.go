// Package apophenia provides seekable pseudo-random numbers, allowing
// reproducibility of pseudo-random results regardless of the order they're
// generated in.
package apophenia

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"math/rand"
)

// Sequence represents a specific deterministic but pseudo-random-ish series
// of bits. A Sequence can be used as a `rand.Source` or `rand.Source64`
// for `math/rand`.
type Sequence interface {
	rand.Source
	Seek(Uint128) Uint128
	BitsAt(Uint128) Uint128
}

// aesSequence128 implements Sequence on top of an AES block cipher.
type aesSequence128 struct {
	key                   [16]byte
	cipher                cipher.Block
	plainText, cipherText [16]byte
	offset                Uint128
	err                   error
}

// NewSequence generates a sequence initialized with the given seed.
func NewSequence(seed int64) Sequence {
	s := aesSequence128{offset: OffsetFor(SequenceRandSource, 0, 0, 0)}
	s.Seed(seed)
	if s.err != nil {
		panic("impossible error: " + s.err.Error())
	}
	return &s
}

// Seed sets the generator to a known state.
func (s *aesSequence128) Seed(seed int64) {
	var newKey [16]byte
	binary.LittleEndian.PutUint64(newKey[:8], uint64(seed))
	newCipher, err := aes.NewCipher(newKey[:])
	if err != nil {
		// we can't return an error, because Seed() can't fail. also
		// note that this can't actually happen, supposedly.
		s.err = err
		return
	}
	copy(s.key[:], newKey[:])
	s.cipher = newCipher
	s.offset = Uint128{0, 0}
}

// Int63 returns a value in 0..(1<<63)-1.
func (s *aesSequence128) Int63() int64 {
	return int64(s.Uint64() >> 1)
}

// Uint64 returns a value in 0..(1<<64)-1.
func (s *aesSequence128) Uint64() uint64 {
	out := s.BitsAt(s.offset)
	s.offset.Inc()
	return out.lo
}

// SequenceClass denotes one of the sequence types, which are used to allow
// sequences to avoid hitting each other's pseudo-random results.
type SequenceClass uint8

const (
	// SequenceDefault is the zero value, used if you didn't think to pick one.
	SequenceDefault SequenceClass = iota
	// SequencePermutationK is the K values for the permutation algorithm.
	SequencePermutationK
	// SequencePermutationF is the F values for the permutation algorithm.
	SequencePermutationF
	// SequenceWeighted is used to generate weighted values for a given
	// position.
	SequenceWeighted
	// SequenceLinear is the random numbers for U%N type usage.
	SequenceLinear
	// SequenceZipfU is the random numbers for the Zipf computations.
	SequenceZipfU
	// SequenceRandSource is used by default when a Sequence is being
	// used as a rand.Source.
	SequenceRandSource
	// SequenceUser1 is eserved for non-apophenia package usage.
	SequenceUser1
	// SequenceUser2 is reserved for non-apophenia package usage.
	SequenceUser2
)

// OffsetFor determines the Uint128 offset for a given class/seed/iteration/id.
func OffsetFor(class SequenceClass, seed uint32, iter uint32, id uint64) Uint128 {
	return Uint128{hi: (uint64(class) << 56) | (uint64(seed) << 24) | uint64(iter),
		lo: id}
}

// Seek seeks to the specified offset, yielding the previous offset. This
// sets the stream to a specific point in its cycle, affecting future calls
// to Int63 or Uint64.
func (s *aesSequence128) Seek(offset Uint128) (old Uint128) {
	s.offset.lo, s.offset.hi, old.lo, old.hi = offset.lo, offset.hi, s.offset.lo, s.offset.hi
	return old
}

// BitsAt yields the sequence of bits at the provided offset into the stream.
func (s *aesSequence128) BitsAt(offset Uint128) (out Uint128) {
	binary.LittleEndian.PutUint64(s.plainText[:8], offset.lo)
	binary.LittleEndian.PutUint64(s.plainText[8:], offset.hi)
	s.cipher.Encrypt(s.cipherText[:], s.plainText[:])
	out.lo, out.hi = binary.LittleEndian.Uint64(s.cipherText[:8]), binary.LittleEndian.Uint64(s.cipherText[8:])
	return out
}
