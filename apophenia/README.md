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

### Sequences

One concern with a seekable PRNG is that if everything is using the
same sequence, you might end up using the same set of bits repeatedly
for, say, a given column input. To avoid this, apophenia has a
suggested (but not enforced) way of mapping the offset space into
sequences, rows, iterations, and columns. The 128-bit "offset" into
the space of possible results is broken down into two 64-bit words:

```
High-order word:
0               1               2               3
0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
[iteration              ][row                          ][seq   ]
Low-order word:
0               1               2               3
0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
[column                                                        ]
```

The convenience function `OffsetFor(sequence, row, iteration, column)`
supports this.

As a side effect, if generating additional values for a given row and
column, you can increment the high-order word of the `Uint128`,
and if generating values for a new column, you can increment the low-order
word.
