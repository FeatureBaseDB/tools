# Imagine that you had a database with...

This tool intends to provide a way to populate a Pilosa database with
predictable contents in a reasonably efficient fashion, without needing
enormous static data files. Indexes and fields within them can be specified
in a TOML file.

## Invocation

The `imagine` utility takes command line options, followed by one or more
spec files, which are TOML files containing specs.

What `imagine` does with the spec files is controlled by the following behavior options:

*  `--describe`             describe the specs
*  `--verify string`        index structure validation: create/error/purge/update/none
*  `--generate`             generate data as specified by workloads
*  `--delete`               delete specified fields

Invoked without behavior options, or with only `--describe`, `imagine` will
describe the indexes and workloads from its spec files, and terminate. If one
or more of verify, generate, or delete is provided, it will do those in order.

The following verification options exist:

* `create`: Attempts to create all specified indexes and fields, errors out
  if any already existed.
* `error`: Verify that indexes and fields exist, error out if they don't.
* `purge`: Delete all existing indexes and fields, then try to create them.
  Error out if either part of this fails.
* `update`: Try to create any missing indexes or fields. Error out if this fails.
* `none`: Do no verification. (Workloads will still check for index/field
  existence.)

The default for `--verify` is determined by other parameters; if `--delete` is
present, and `--generate` is not, the default verification is "none" (there's
no point in verifying that things exist right before deleting them), otherwise
the default verification is "error".

The following options change how `imagine` goes about its work:

*  `--column-scale int`     scale number of columns provided by specs
*  `--cpu-profile string`   record CPU profile to file
*  `--dry-run`              dry-run; describe what would be done
*  `--hosts string`         comma-separated host names for Pilosa server (default "localhost")
*  `--mem-profile string`   record allocation profile to file
*  `--port int`             host port for Pilosa server (default 10101)
*  `--prefix string`        prefix to use on index names
*  `--row-scale int`        scale number of rows provided by specs
*  `--thread-count int`     number of threads to use for import, overrides value in config file (default 1)
*  `--time`                 report on time elapsed for operations

## Spec files

The following global settings exist for each spec:

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

A spec can specify two other kinds of things, indexes and workloads.
Indexes describe the data that will go in a Pilosa index, such as the index's
name, size (in columns), and number of fields. Workloads describe specific
patterns of creating and inserting data in fields.

When multiple specs are provided, they are combined. Indexes and fields are
merged; any conflicts between them are an error, and `imagine` will report
such errors and then stop. Workloads are concatenated, with specs processed
in command-line order.

### Indexes

Indexes are defined in a top-level map, using the index name as the key. Each
index would typically be written as `[indexes.indexname]`. Each index has
settings, plus field entries under the index. Fields are a mapping of names
to field specifications.

* name: The index's name. (This will be prefixed later.)
* description: A longer description of the index's purpose within a set.
* columns: The number of columns.
* seed: A default PRNG seed to use for fields that don't specify their own.

### Fields

Fields can take several kinds, specified as "type". Defined types:
* `set`: The default "set" field type, where rows correspond to specific
  values.
* `mutex`: The "mutex" field type, which is like a set, only it enforces
  that only one row is set per column.
* `int`: The binary-representation field type, usable for range queries.
* `time`: The "time" field type, which is a set with additional optional
  timestamp information.

All fields share some common parameters:

* `zipfV`, `zipfS`: the V/S values used for a Zipf distribution of values.
* `min`, `max`: Minimum and maximum values. For int fields, this is the value
  range; for set/mutex fields, it's the range of rows that will be
  potentially generated.
* `sourceIndex`: An index to use for values; the value range will be the
  source index's column range. If source index has 100,000 columns, this
  is equivalent to "min: 0, max: 99999".
* `density`: The field's base density of bits set. For a set, this density
  applies to each row independently; for a mutex or int field, it
  determines how many columns should have a value set.
* `valueRule`: "linear" or "zipf". Exact interpretation varies by field type,
  but "linear" indicates that all rows should have the same density of
  values, while "zipf" indicates that they should follow a Zipf distribution.

#### Set/Mutex Fields

Set and mutex fields can also configure a cache type:

* `cache`: Cache type, one of "lru" or "none".

##### Set/Time Fields

Set (and time) fields can be generated either in row-major order (generate
one row at a time for all columns) or column-major order (generate all rows for
each column).

* `dimensionOrder`: string, one of "row" or "column". default is "row".
* `quantum`: string, one of "Y", "YM", "YMD", or "YMDH". Valid only for
  time fields. Default is "YMDH".

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

#### Int Fields

By default, every member of a int field is set to a random value within the
range.

Zipf parameters: This follows the behavior of the Zipf generator in `math/rand`,
with an offset of the minimum value. For instance, a field with min/max of
10/20 behaves exactly like a field with a min/max of 0/10, with 10 added to
each value.

### Workloads

A workload describes a named series of steps, which apply to indexes
and fields previously described. Workloads don't have to be in the same
spec files as the indexes and fields they refer to. Workloads are defined
in a top-level array, usually using `[[workloads]]` to refer to them.
Workloads are sequential. They have the following attributes:

* `name`: The name of the workload.
* `description`: A description of the workload.
* `threadCount`: Number of importer threads to use in imports.
* `batchSize`: The default size of import batches (number of records before
  the client transmits records to the server).

Each workload also has one or more "batches", which are sets of tasks.
Batches are executed sequentially in order within each workload. They
are introduced using `[[workloads.batches]]`.

#### Batches

Each batch has optional configuration fields, plus an array of tasks.

* `description`: A description of the workload.
* `threadCount`: Number of importer threads to use in imports.
* `batchSize`: Size of import batches (overrides, but defaults to,
  workload's batchSize).

The `threadCount` value for a batch overrides the parent workload's default,
and is used for each task within the batch. Additionally, the tasks within a
batch are all executed in parallel.

#### Tasks

Each task outlines a specific set of data to populate in a given field.

* `index`, `field`: the index and field names to identify the field to be
  populated. The index name should match the name in the spec, not including
  any prefixes.
* `seed`: the random number seed to use when populating this field. Defaults
  to the seed for the field's parent index.
* `columns`: the number of columns to populate. default: populate the
  entire field, using the index's columns.
* `columnOffset`: column to start with. The special value "append" means
  to create new columns starting immediately after the highest column
  previously created.
* `columnOrder`: "linear", "stride", "zipf", or "permute" (default linear).
  Indicates order in which to generate column values.
* `stride`: The stride to use with a columnOrder of "stride".
* `rowOrder`: "linear" or "permute" (default linear). Determines the order
  in which row values are computed, for set fields, or whether to permute
  generated values, for mutex or int fields.
* `batchSize`: Size of import batches (overrides, but defaults to,
  batch's batchSize).
* `stamp`: Controls timestamp behavior. One of "none", "random", "increasing".
* `stampRange`: A duration over which to spread timestamps when generating
  them.
* `stampStart`: A specific time to start timestamps at. Defaults to current
  time minus stamp range.
* `zipfV`, `zipfS`: V and S values for a zipf distribution of columns.
* `zipfRange`: The range to use for the zipf distribution (defaults to
  `columns`).

As a special case, when `columnOffset` is "append" and `columnOrder` is "zipf",
values are randomly generated using a zipf distribution over [0,`zipfRange`).
These values are then subtracted from the *next* column number -- the lowest
column number not currently known to `imagine`, to produce a range which might
indicate updates to existing columns, or might indicate a new column. A value
is generated for each column. Note that this will pick values the same way
mutex or int fields do, rather than generating all the values for each column,
and the same column may be generated more than once. This behavior attempts
to simulate likely behavior for event streams.

The "zipf" `columnOrder` is not supported except with `columnOffset` of
"append", and the Zipf parameters are not defined for any other column order.

## Data Generation

Reproducible data generation means being able to generate the same bits every
time. To this end, we use a seekable PRNG -- you can specify an offset into
its stream and get the same bits every time. See the related package
`apophenia` for details.

Set values are computed using `apophenia.Weighted`, with `seed` equal to the
row number, and `id` equal to the column number.

Mutex/Int: Mutex and int fields both generate a single value in their range.
Linear values are computed using `row` 0, `iter` 0, and are computed as
`min + U % (max - min)`. (For a mutex, the minimum value is always 0.) Zipf
values are computed using iterated values for `row` 0 as inputs to another
algorithm which treats them as [0,1) range values. If RowOrder is set to
`permute`, the permutation is computed using permutation row 2.

Permuted column values are generated by requesting a permutation generator for
row 0 with the given seed. Permuted row values for sets are generated using
a permutation generator for row 1.
