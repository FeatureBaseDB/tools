package main

import (
	"errors"
	"fmt"
	"io"
	"math"

	pilosa "github.com/pilosa/go-pilosa"
	"github.com/pilosa/tools/apophenia"
)

type genfunc func(*taskSpec) (CountingIterator, error)

var newGenerators = map[fieldType]genfunc{
	fieldTypeSet:   newSetGenerator,
	fieldTypeMutex: newMutexGenerator,
	fieldTypeBSI:   newBSIGenerator,
}

// A generator needs to be able to generate columns and rows, sequentially.
// To do this, it needs to know how many things it's generating, and which
// thing it's on. But it also has to be able to mess with orders
// and probabilities.

// CountingIterator represents a pilosa.RecordIterator which additionally
// reports back how many values it's generated, useful for reporting on
// what was done and seeing how number of bits (as opposed to number
// of columns/rows) is affecting performance.
type CountingIterator interface {
	pilosa.RecordIterator
	Values() (int64, int64)
}

// NewGenerator makes a generator which will generate the values for the
// given task.
func NewGenerator(ts *taskSpec) (CountingIterator, []pilosa.ImportOption, error) {
	if ts == nil {
		return nil, nil, errors.New("nil field spec is invalid")
	}
	fn := newGenerators[ts.FieldSpec.Type]
	if fn == nil {
		return nil, nil, fmt.Errorf("field spec: invalid field type %v", ts.FieldSpec.Type)
	}
	opts := make([]pilosa.ImportOption, 0, 8)
	if ts.BatchSize != nil {
		opts = append(opts, pilosa.OptImportBatchSize(*ts.BatchSize))
	}
	if ts.Parent.ThreadCount != nil {
		opts = append(opts, pilosa.OptImportThreadCount(*ts.Parent.ThreadCount))
	}
	if noSortNeeded(ts) {
		opts = append(opts, pilosa.OptImportSort(false))
	}
	iter, err := fn(ts)
	return iter, opts, err
}

func noSortNeeded(ts *taskSpec) bool {
	switch {
	case ts.ColumnOrder == valueOrderPermute, ts.RowOrder == valueOrderPermute:
		return false
	default:
		return true
	}
}

// Three cases:
// BSI: FieldValue, one per column.
// Mutex: Column, one per column.
// Set: FieldValue, possibly many per column, possibly column-major.

func newSetGenerator(ts *taskSpec) (iter CountingIterator, err error) {
	fs := ts.FieldSpec
	dvg := doubleValueGenerator{}
	dvg.colGen, err = makeColumnGenerator(ts)
	if err != nil {
		return nil, err
	}
	dvg.rowGen, err = makeRowGenerator(ts)
	if err != nil {
		return nil, err
	}
	dvg.densityGen, dvg.densityPerCol = makeDensityGenerator(fs, *ts.Seed)
	dvg.densityScale = *fs.DensityScale
	dvg.weighted, err = apophenia.NewWeighted(apophenia.NewSequence(*ts.Seed))

	switch ts.DimensionOrder {
	case dimensionOrderRow:
		dvg.colDone = true
		return &rowMajorValueGenerator{doubleValueGenerator: dvg}, nil
	case dimensionOrderColumn:
		dvg.rowDone = true
		return &columnMajorValueGenerator{doubleValueGenerator: dvg}, nil
	}
	return nil, errors.New("unknown dimension order for set")
}

func newMutexGenerator(ts *taskSpec) (iter CountingIterator, err error) {
	cvg := columnValueGenerator{}
	cvg.colGen, err = makeColumnGenerator(ts)
	if err != nil {
		return nil, err
	}
	cvg.valueGen, err = makeValueGenerator(ts)
	if err != nil {
		return nil, err
	}
	return &cvg, nil
}

func newBSIGenerator(ts *taskSpec) (iter CountingIterator, err error) {
	fvg := fieldValueGenerator{}
	fvg.colGen, err = makeColumnGenerator(ts)
	if err != nil {
		return nil, err
	}
	fvg.valueGen, err = makeValueGenerator(ts)
	if err != nil {
		return nil, err
	}
	return &fvg, nil
}

// makeColumnGenerator builds a generator to iterate over columns of a field
func makeColumnGenerator(ts *taskSpec) (sequenceGenerator, error) {
	switch ts.ColumnOrder {
	case valueOrderStride:
		return newStrideGenerator(int64(ts.Stride), int64(ts.FieldSpec.Parent.Columns), int64(*ts.Columns)), nil
	case valueOrderLinear:
		return newIncrementGenerator(0, int64(*ts.Columns)), nil
	case valueOrderPermute:
		// "row 0" => column permutations, "row 1" => row permutations
		gen, err := newPermutedGenerator(0, int64(ts.FieldSpec.Parent.Columns), int64(*ts.Columns), 0, *ts.Seed)
		if err != nil {
			return nil, err
		}
		return gen, nil
	}
	return nil, errors.New("unknown column generator type")
}

// makeRowGenerator builds a generator to iterate over columns of a field
func makeRowGenerator(ts *taskSpec) (sequenceGenerator, error) {
	fs := ts.FieldSpec
	switch ts.RowOrder {
	case valueOrderStride:
		return newStrideGenerator(int64(ts.Stride), int64(fs.Max), int64(fs.Max)), nil
	case valueOrderLinear:
		return newIncrementGenerator(0, int64(fs.Max)), nil
	case valueOrderPermute:
		// "row 0" => column permutations, "row 1" => row permutations
		gen, err := newPermutedGenerator(0, fs.Max, fs.Max, 1, *ts.Seed)
		if err != nil {
			return nil, err
		}
		return gen, nil
	}
	return nil, errors.New("unknown row generator type")
}

func makeValueGenerator(ts *taskSpec) (vg valueGenerator, err error) {
	fs := ts.FieldSpec
	switch fs.ValueRule {
	case densityTypeLinear:
		vg, err = newLinearValueGenerator(fs.Min, fs.Max, *ts.Seed)
	case densityTypeZipf:
		vg, err = newZipfValueGenerator(fs.ZipfS, fs.ZipfV, fs.Min, fs.Max, *ts.Seed)
	default:
		err = errors.New("unknown value generator type")
	}
	if ts.RowOrder == valueOrderPermute && err == nil {
		vg, err = permuteValueGenerator(vg, fs.Min, fs.Max, *ts.Seed)
	}
	return vg, err
}

// sequenceGenerator represents something that iterates through a
// range or series. It runs until done, then resets on further calls.
// For example, a sequenceGenerator generating 1..3 would generate:
// 1 false
// 2 false
// 3 true
// 1 false
// 2 false
// 3 true
// [...]
type sequenceGenerator interface {
	Next() (value int64, done bool)
}

// incrementGenerator counts from min to max repeatedly.
type incrementGenerator struct {
	current, min, max int64
}

// Next returns the next value in a sequence.
func (ig *incrementGenerator) Next() (value int64, done bool) {
	value = ig.current
	ig.current++
	if ig.current >= ig.max {
		ig.current = ig.min
		done = true
	}
	return value, done
}

func newIncrementGenerator(min, max int64) *incrementGenerator {
	return &incrementGenerator{current: min, min: min, max: max}
}

// incrementGenerator counts from min to max repeatedly.
type strideGenerator struct {
	current, stride, max int64
	emitted, total       int64
}

// Next returns the next value in a sequence.
func (ig *strideGenerator) Next() (value int64, done bool) {
	value = ig.current
	ig.current += ig.stride
	if ig.current >= ig.max {
		// drop all multiples of ig.stride
		ig.current %= ig.stride
		// do a different batch. if ig.current becomes equal to ig.stride,
		// we'll be done -- but that should be caught by the emitted count anyway.
		ig.current++
	}
	ig.emitted++
	if ig.emitted >= ig.total {
		ig.emitted = 0
		ig.current = 0
		done = true
	}
	return value, done
}

func newStrideGenerator(stride, max, total int64) *strideGenerator {
	return &strideGenerator{current: 0, stride: stride, max: max, total: total}
}

type permutedGenerator struct {
	permutation    *apophenia.Permutation
	offset         int64
	current, total int64
}

// Next generates a new value from an underlying sequence.
func (pg *permutedGenerator) Next() (value int64, done bool) {
	value = pg.current
	pg.current++
	if pg.current >= pg.total {
		pg.current = 0
		done = true
	}
	// permute value, and coerce it back to range
	value = pg.permutation.Nth(value) + pg.offset
	return value, done
}

func newPermutedGenerator(min, max, total int64, row uint32, seed int64) (*permutedGenerator, error) {
	var err error
	seq := apophenia.NewSequence(seed)
	pg := &permutedGenerator{offset: min, total: total}
	pg.permutation, err = apophenia.NewPermutation(max-min, row, seq)
	return pg, err
}

// valueGenerator represents a thing which generates predictable values
// for a sequence. Used for mutex/BSI fields.
type valueGenerator interface {
	Nth(int64) int64
}

// linearValueGenerator generates values with approximately equal probabilities
// within their range.
type linearValueGenerator struct {
	seq       apophenia.Sequence
	bitoffset apophenia.Uint128
	offset    int64
	max       uint64
}

func newLinearValueGenerator(min, max, seed int64) (*linearValueGenerator, error) {
	lvg := &linearValueGenerator{offset: min, max: uint64(max - min), seq: apophenia.NewSequence(seed)}
	lvg.bitoffset = apophenia.OffsetFor(apophenia.SequenceUser1, 0, 0, 0)
	return lvg, nil
}

func (lvg *linearValueGenerator) Nth(n int64) int64 {
	lvg.bitoffset.Lo = uint64(n)
	val := lvg.seq.BitsAt(lvg.bitoffset).Lo % lvg.max
	return int64(val) + lvg.offset
}

// zipfValueGenerator generator generates values with a Zipf distribution.
type zipfValueGenerator struct {
	z      *apophenia.Zipf
	offset int64
}

func newZipfValueGenerator(s, v float64, min, max, seed int64) (*zipfValueGenerator, error) {
	var err error
	zvg := zipfValueGenerator{offset: min}
	zvg.z, err = apophenia.NewZipf(s, v, uint64(max-min), 0, apophenia.NewSequence(seed))
	if err != nil {
		return nil, err
	}
	return &zvg, nil
}

func (zvg *zipfValueGenerator) Nth(n int64) int64 {
	val := zvg.z.Nth(uint64(n))
	return int64(val) + zvg.offset
}

type permutedValueGenerator struct {
	base     valueGenerator
	permuter *apophenia.Permutation
	offset   int64
}

func permuteValueGenerator(vg valueGenerator, min, max, seed int64) (*permutedValueGenerator, error) {
	var err error
	seq := apophenia.NewSequence(seed)
	nvg := permutedValueGenerator{base: vg, offset: min}
	// 2 is an arbitrary magic number; we used 0 and 1 for other permutation sequences.
	nvg.permuter, err = apophenia.NewPermutation(max-min, 2, seq)
	return &nvg, err
}

func (pvg *permutedValueGenerator) Nth(n int64) int64 {
	val := pvg.base.Nth(n)
	val -= pvg.offset
	val = pvg.permuter.Nth(val)
	val += pvg.offset
	return val
}

type singleValueGenerator struct {
	colGen    sequenceGenerator
	valueGen  valueGenerator
	values    int64
	tries     int64
	completed bool
}

func (svg *singleValueGenerator) Values() (int64, int64) {
	return svg.values, svg.tries
}

// Iterate loops over columns, producing a value for each column.
func (svg *singleValueGenerator) Iterate() (column int64, value int64, done bool) {
	column, done = svg.colGen.Next()
	value = svg.valueGen.Nth(column)
	svg.completed = done
	return column, value, done
}

type fieldValueGenerator struct {
	singleValueGenerator
}

// NextRecord returns the next value pair from the fieldValueGenerator,
// as a pilosa.FieldValue.
func (fvg *fieldValueGenerator) NextRecord() (pilosa.Record, error) {
	if fvg.completed {
		return nil, io.EOF
	}
	col, val, _ := fvg.Iterate()
	fvg.tries++
	fvg.values++
	return pilosa.FieldValue{ColumnID: uint64(col), Value: val}, nil
}

type columnValueGenerator struct {
	singleValueGenerator
}

// NextRecord returns the next value pair from the columnValueGenerator,
// as a pilosa.Column.
func (cvg *columnValueGenerator) NextRecord() (pilosa.Record, error) {
	if cvg.completed {
		return nil, io.EOF
	}
	col, val, _ := cvg.Iterate()
	cvg.tries++
	cvg.values++
	return pilosa.Column{ColumnID: uint64(col), RowID: uint64(val)}, nil
}

type densityGenerator interface {
	Density(col, row uint64) uint64
}

type fixedDensityGenerator uint64

func (f *fixedDensityGenerator) Density(col, row uint64) uint64 {
	return uint64(*f)
}

type zipfDensityGenerator struct {
	base, zipfV, zipfS, scale float64
}

func (z *zipfDensityGenerator) Density(col, row uint64) uint64 {
	// from the README as of when I wrote this:
	// For instance, with v=2, s=2, the k=0 probability is proportional to
	// `(2+0)**(-2)` (1/4), and the k=1 probability is proportional to
	// `(2+1)**(-2)` (1/9). Thus, the probability of a bit being set in the k=1 row is
	// 4/9 the base density.
	proportion := math.Pow(float64(row)+z.zipfV, -z.zipfS)
	return uint64(z.base * proportion * z.scale)
}

// maybeDensityGenerator tries itself or the next density generator in line to
// produce a value.
type maybeDensityGenerator struct {
	chance, scale   uint64
	generator, next densityGenerator
	weighted        *apophenia.Weighted
}

func newMaybeDensityGenerator(fs *fieldSpec, seed int64) *maybeDensityGenerator {
	var err error
	m := maybeDensityGenerator{chance: uint64(float64(*fs.DensityScale) * *fs.Chance), scale: *fs.DensityScale}
	m.weighted, err = apophenia.NewWeighted(apophenia.NewSequence(seed))
	if err != nil {
		return nil
	}
	if fs.Next != nil {
		m.next, _ = makeDensityGenerator(fs.Next, seed)
	}
	m.generator = baseDensityGenerator(fs)
	return &m
}

func (m *maybeDensityGenerator) Density(col, row uint64) uint64 {
	if m == nil {
		return 0
	}
	// we ignore row here, because we want to get the same selection of density
	// for a given column every time.
	offset := apophenia.OffsetFor(apophenia.SequenceWeighted, 0, 0, uint64(col))
	bit := m.weighted.Bit(offset, m.chance, m.scale)
	if bit == 1 {
		return m.generator.Density(col, row)
	}
	if m.next != nil {
		return m.next.Density(col, row)
	}
	return 0
}

func baseDensityGenerator(fs *fieldSpec) densityGenerator {
	switch fs.ValueRule {
	case densityTypeLinear:
		fdg := fixedDensityGenerator(float64(*fs.DensityScale) * fs.Density)
		return &fdg
	case densityTypeZipf:
		return &zipfDensityGenerator{base: fs.Density / math.Pow(fs.ZipfV, -fs.ZipfS), zipfV: fs.ZipfV, zipfS: fs.ZipfS, scale: float64(*fs.DensityScale)}
	}
	return nil
}

func makeDensityGenerator(fs *fieldSpec, seed int64) (densityGenerator, bool) {
	if *fs.Chance != 1.0 {
		return newMaybeDensityGenerator(fs, seed+1), true
	}
	return baseDensityGenerator(fs), false
}

// for sets, we have to iterate over columns and then rows, or rows and
// then columns.

type doubleValueGenerator struct {
	colGen, rowGen   sequenceGenerator
	colDone, rowDone bool
	densityGen       densityGenerator
	densityScale     uint64
	densityPerCol    bool
	density          uint64
	weighted         *apophenia.Weighted
	row, col         int64
	values           int64
	tries            int64
}

// Values yields the number of values generated, and also the number of
// positions evaluated.
func (dvg *doubleValueGenerator) Values() (int64, int64) {
	return dvg.values, dvg.tries
}

type rowMajorValueGenerator struct {
	doubleValueGenerator
}

// NextRecord() finds the next record, probably.
func (rvg *rowMajorValueGenerator) NextRecord() (pilosa.Record, error) {
	for !rvg.colDone || !rvg.rowDone {
		if rvg.colDone {
			rvg.row, rvg.rowDone = rvg.rowGen.Next()
			if !rvg.densityPerCol {
				rvg.density = rvg.densityGen.Density(uint64(rvg.col), uint64(rvg.row))
			}
		}
		rvg.col, rvg.colDone = rvg.colGen.Next()
		if rvg.densityPerCol {
			rvg.density = rvg.densityGen.Density(uint64(rvg.col), uint64(rvg.row))
		}
		// use row as the "seed" for Weighted computations, so each row
		// can have different values.
		offset := apophenia.OffsetFor(apophenia.SequenceWeighted, uint32(rvg.row), 0, uint64(rvg.col))
		bit := rvg.weighted.Bit(offset, rvg.density, rvg.densityScale)
		rvg.tries++
		if bit != 0 {
			rvg.values++
			return pilosa.Column{ColumnID: uint64(rvg.col), RowID: uint64(rvg.row)}, nil
		}
	}
	return nil, io.EOF
}

type columnMajorValueGenerator struct {
	doubleValueGenerator
}

func (rvg *columnMajorValueGenerator) NextRecord() (pilosa.Record, error) {
	for !rvg.colDone || !rvg.rowDone {
		if rvg.rowDone {
			rvg.col, rvg.colDone = rvg.colGen.Next()
		}
		rvg.row, rvg.rowDone = rvg.rowGen.Next()
		offset := apophenia.OffsetFor(apophenia.SequenceWeighted, uint32(rvg.row), 0, uint64(rvg.col))
		density := rvg.densityGen.Density(uint64(rvg.col), uint64(rvg.row))
		bit := rvg.weighted.Bit(offset, density, rvg.densityScale)
		rvg.tries++
		if bit != 0 {
			rvg.values++
			return pilosa.Column{ColumnID: uint64(rvg.col), RowID: uint64(rvg.row)}, nil
		}
	}
	return nil, io.EOF
}
