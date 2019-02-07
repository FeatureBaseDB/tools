# Imagine that you had a database with...

This tool intends to provide a way to populate a Pilosa database with
predictable contents in a reasonably efficient fashion, without needing
enormous static data files. Indexes and fields within them can be specified
in a TOML file.

## Global settings

The following global settings exist:

    * densityscale: A density scale factor used to determine the precision
      used for density computations. Density scale should be a power of two.
      Higher density scales will take longer to compute and process. (The
      operation is O(log2(N)).)
    * prefix: A preferred prefix to use for index names. If absent,
      `imaginary` is used. The `--prefix` command line option overrides
      this.
    * version: The string "1.0". The intent is that future versions of the
      tool will attempt to ensure that a given spec produces identical results.
      If a later version would change the results from a spec, it should do so
      only when a different string is specified here. However, *this
      guarantee is not yet in force*. The software is still in an immature
      state, and may change output significantly during development.
    * seed: A default PRNG seed, used for indexes/fields that don't specify
      their own.

Indexes are defined in a top-level array, usually written as `[[indexes]]`.

## Indexes

Each index has settings, plus field entries under the index. Fields are
a mapping of names to field specifications.

    * name: The index's name. (This will be prefixed later.)
    * description: A longer description of the index's purpose within a set.
    * columns: The number of columns.
    * seed: A default PRNG seed to use for fields that don't specify their own.
    * threadCount: Number of importer threads. (Experimental feature for
      internal benchmarking work.)

### Fields

Fields can take several kinds, specified as "type". Defined types:
    * "set" => The default "set" field type, where rows correspond to specific
      values.
    * "mutex" => The "mutex" field type, which is like a set, only it enforces
      that only one row is set per column.
    * "bsi" => The binary-representation field type, usable for range queries.

All fields share some common parameters:

    * columns: the number of columns to populate. default: populate the
      entire field, using the index's columns. Must not be higher than
      the index's number of columns.
    * columnOrder: "linear", "stride" or "permute" (default linear). Indicates
      whether column values should be generated sequentially or in permuted
      order.
    * stride: The stride to use with a columnOrder of "stride".
    * zipfV, zipfS: the V/S values used for a Zipf distribution of values.
    * seed: The PRNG seed to use for this field. Defaults to the index's
      seed.
    * min, max: Minimum and maximum values. For BSI fields, this is the value
      range; for set/mutex fields, it's the range of rows that will be
      potentially generated.
    * sourceIndex: An index to use for values; the value range will be the
      source index's column range. If source index has 100,000 columns, this
      is equivalent to "min: 0, max: 99999".
    * density: The field's base density of bits set. For a set, this density
      applies to each row independently; for a mutex or BSI field, it
      determines how many columns should have a value set. (If a stride is
      also specified, only the columns determined by the stride are checked,
      but density is also checked to decide which ones to set.)
    * valueRule: "linear" or "zipf". Exact interpretation varies by field type,
      but "linear" indicates that all rows should have the same density of
      values, while "zipf" indicates that they should follow a Zipf distribution.
    * rowOrder: "linear" or "permute" (default linear). Determines the order
      in which row values are computed, for set fields, or whether to permute
      generated values, for mutex or BSI fields.

#### Set/Mutex Fields

Set and mutex fields can also configure a cache type:

    * cache: Cache type, one of "lru" or "none".

##### Set Fields

Set fields can be generated either in row-major order (generate one row at
a time for all columns) or column-major order (generate all rows for each
column).

    * dimensionOrder: string, one of "row" or "column". default is "row".

Zipf parameters: The first row will have bits set based on the base density
provided. Following rows will follow the Zipf distribution's probabilities.
For instance, with v=2, s=2, the k=0 probability is proportional to
`(2+0)**(-2)` (1/4), and the k=1 probability is proportional to
`(2+1)**(-2)` (1/9). Thus, the probability of a bit being set in the k=1 row is
4/9 the base density.

The final set of bits does not depend on whether values were computed in
rowMajor order. (This guarantee is slightly weaker than other guarantees.)

##### Mutex Fields

Zipf parameters: This just follows the behavior of the Zipf generator in
`math/rand`. A single value is determined for each column, determining which
bit is set.

#### BSI Fields

By default, every member of a BSI field is set to a random value within the
range.

Zipf parameters: This follows the behavior of the Zipf generator in `math/rand`,
with an offset of the minimum value. For instance, a field with min/max of
10/20 behaves exactly like a field with a min/max of 0/10, with 10 added to
each value.

## Data Generation

Reproducible data generation means being able to generate the same bits every
time. To this end, we use a seekable PRNG -- you can specify an offset into
its stream and get the same bits every time. See the related package
`apophenia` for details.

Set values are computed using `apophenia.Weighted`, with `seed` equal to the
row number, and `id` equal to the column number.

Mutex/BSI: Mutex and BSI fields both generate a single value in their range.
Linear values are computed using `row` 0, `iter` 0, and are computed as
`min + U % (max - min)`. (For a mutex, the minimum value is always 0.) Zipf
values are computed using iterated values for `row` 0 as inputs to another
algorithm which treats them as [0,1) range values. If RowOrder is set to
`permute`, the permutation is computed using permutation row 2.

Permuted column values are generated by requesting a permutation generator for
row 0 with the given seed. Permuted row values for sets are generated using
a permutation generator for row 1.
