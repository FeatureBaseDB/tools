# Apophenia -- seeking patterns in randomness

Apophenia provides an approximate emulation of a seekable pseudo-random
number generator. You provide a seed, and get a generator which can generate
a large number of pseudo-random bits which will occur in a predictable
pattern, but you can seek anywhere in that pattern in constant time.

Apophenia's interface is intended to be similar to that of stdlib's
`math/rand`. In fact, the `Sequence` interface type is a strict superset
of `rand.Source`.

## Implementation Notes

AES-128, for a given seed, consists of a PRNG which, from 2^128 input
coordinates, produces 2^128 possible outputs, each of which is 128 bits
long. This is equivalent to a single-bit PRNG of period 2^135. It is
also possible to treat the input 128 bits as a coordinate system of
some sort, to allow multiple parallel sequences, etcetera.

This design may have serious fundamental flaws, but it worked out in
light testing and I'm an optimist.

### Sequences and Offsets

Apophenia's underlying implementation admits 128-bit keys, and 128-bit
offsets within each sequence. In most cases:

    * That's more space than we need.
    * Working with a non-native type for item numbers is annoying,
      but 64-bits is enough range.
    * It would be nice to avoid using the *same* pseudo-random values
      for different things.
    * Even when those things have the same basic identifying ID or
      value.

For instance, say you wish to generate a billion items. Each item should
have several "random" values. Some values might follow a Zipf distribution,
others might just be "U % N" for some N. If you use the item number as a
key, and seek to the same position for each of these, and get the same bits
for each of these, you may get unintended similarities or correlations between
them.

With this in mind, apophenia divides its 128-bit offset space into a number
of spaces. The most significant bits are used for a sequence-type value, one
of:

    * SequenceDefault
    * SequencePermutationK/SequencePermutationF: permutations
    * SequenceWeighted: weighted bits
    * SequenceLinear: linear values within a range
    * SequenceZipfU: uniforms to use for Zipf values
    * SequenceRandSource: default offsets for the rand.Source
    * SequenceUser1/SequenceUser2: reserved for non-apophenia usage

Other values are not yet defined, but are reserved.

Within most of these spaces, the rest of the high word of the offset is used
for a 'seed' (used to select different sequences) and an 'iteration' (used
for successive values consumed by an algorithm). The low-order word is treated
as a 64-bit item ID.

```
High-order word:
0               1               2               3
0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
[iteration              ][seed                         ][seq   ]
Low-order word:
0               1               2               3
0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
[id                                                            ]
```

The convenience function `OffsetFor(sequence, seed, iteration, id)`
supports this usage.

As a side effect, if generating additional values for a given seed and
id, you can increment the high-order word of the `Uint128`,
and if generating values for a new id, you can increment the low-order
word. If your algorithm consumes more than 2^24 values for a single
operation, you could start hitting values shared with other seeds. Oh,
well.

#### Iteration usage

For the built-in consumers:

    * Weighted consumes log2(scale) iterated values.
    * Zipf consumes an *average* of no more than about 1.1 values.
    * Permutation consumes one iterated value per 128 rounds of permutation,
      where rounds is equal to `6*ceil(log2(max))`. (For instance, a second
      value is consumed around a maximum of 2^22, and a third around 2^43.)
    * Nothing else uses more than one iterated value.
