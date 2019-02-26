package main

import (
	"errors"
	"fmt"
	"io"
	"math"

	pilosa "github.com/pilosa/go-pilosa"
	"github.com/pilosa/tools/apophenia"
)

type genfunc func(*taskSpec, chan taskUpdate, string) (CountingIterator, error)

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
func NewGenerator(ts *taskSpec, updateChan chan taskUpdate, updateID string) (CountingIterator, []pilosa.ImportOption, error) {
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
	iter, err := fn(ts, updateChan, updateID)
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

func newSetGenerator(ts *taskSpec, updateChan chan taskUpdate, updateID string) (iter CountingIterator, err error) {
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
	dvg.updateChan = updateChan
	dvg.updateID = updateID

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

// newSingleValueGenerator handles the column/value/weighted parts of
// generation that are shared between column/field generators.
func newSingleValueGenerator(ts *taskSpec, updateChan chan taskUpdate, updateID string) (svg singleValueGenerator, err error) {
	svg.colGen, err = makeColumnGenerator(ts)
	if err != nil {
		return svg, err
	}
	svg.valueGen, err = makeValueGenerator(ts)
	if err != nil {
		return svg, err
	}
	if ts.FieldSpec.Density != 1.0 {
		svg.weighted, err = apophenia.NewWeighted(apophenia.NewSequence(*ts.Seed))
		svg.density = uint64(ts.FieldSpec.Density * float64(*ts.FieldSpec.DensityScale))
		svg.scale = *ts.FieldSpec.DensityScale
	}
	svg.updateChan = updateChan
	svg.updateID = updateID
	return svg, err
}

// newMutexGenerator builds a mutex generator, which is a generator
// that computes a single value for a column, then returns it as a
// pilosa.Column.
func newMutexGenerator(ts *taskSpec, updateChan chan taskUpdate, updateID string) (iter CountingIterator, err error) {
	svg, err := newSingleValueGenerator(ts, updateChan, updateID)
	if err != nil {
		return nil, err
	}
	cvg := columnValueGenerator{singleValueGenerator: svg}
	return &cvg, nil
}

// newBSIGenerator builds a value generator, which is a generator
// that computes a single value for a column, then returns it as a
// pilosa.FieldValue.
func newBSIGenerator(ts *taskSpec, updateChan chan taskUpdate, updateID string) (iter CountingIterator, err error) {
	svg, err := newSingleValueGenerator(ts, updateChan, updateID)
	if err != nil {
		return nil, err
	}
	fvg := fieldValueGenerator{singleValueGenerator: svg}
	return &fvg, nil
}

// makeColumnGenerator builds a generator to iterate over columns of a field.
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

// makeRowGenerator builds a generator to iterate over rows of a field.
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

// makeValueGenerator makes a generator which generates values for fields which
// can only have one value per column, such as mutex/BSI fields.
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
		vg, err = newPermutedValueGenerator(vg, fs.Min, fs.Max, *ts.Seed)
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
	Status() (produced, total int64)
}

// incrementGenerator counts from min to max by 1.
type incrementGenerator struct {
	produced, current, min, max int64
}

// Next returns the next value in a sequence.
func (g *incrementGenerator) Next() (value int64, done bool) {
	value = g.current
	g.current++
	g.produced++
	if g.current >= g.max {
		g.current = g.min
		done = true
	}
	return value, done
}

// Status reports on the state of the generator.
func (g *incrementGenerator) Status() (produced, total int64) {
	return g.produced, g.max
}

// newIncrementGenerator creates an incrementGenerator.
func newIncrementGenerator(min, max int64) *incrementGenerator {
	return &incrementGenerator{current: min, min: min, max: max}
}

// strideGenerator counts from min to max by multiples of stride, then
// from min+1 to (max+1-stride), and so on, until it has covered the whole
// range.
type strideGenerator struct {
	current, stride, max int64
	emitted, total       int64
}

// Next returns the next value in a sequence.
func (g *strideGenerator) Next() (value int64, done bool) {
	value = g.current
	g.current += g.stride
	if g.current >= g.max {
		// drop all multiples of ig.stride
		g.current %= g.stride
		// do a different batch. if ig.current becomes equal to ig.stride,
		// we'll be done -- but that should be caught by the emitted count anyway.
		g.current++
	}
	g.emitted++
	if g.emitted >= g.total {
		g.emitted = 0
		g.current = 0
		done = true
	}
	return value, done
}

// Status reports on the state of the generator.
func (g *strideGenerator) Status() (produced, total int64) {
	return g.emitted, g.total
}

// newStrideGenerator produces a stride generator.
func newStrideGenerator(stride, max, total int64) *strideGenerator {
	return &strideGenerator{current: 0, stride: stride, max: max, total: total}
}

// permutedGenerator generates things in a range in an arbitrary order.
type permutedGenerator struct {
	permutation    *apophenia.Permutation
	offset         int64
	current, total int64
}

// Next generates a new value from an underlying sequence.
func (g *permutedGenerator) Next() (value int64, done bool) {
	value = g.current
	g.current++
	if g.current >= g.total {
		g.current = 0
		done = true
	}
	// permute value, and coerce it back to range
	value = g.permutation.Nth(value) + g.offset
	return value, done
}

// Status reports on the state of the generator.
func (g *permutedGenerator) Status() (produced, total int64) {
	return g.current, g.total
}

// newPermutedGenerator creates a permutedGenerator.
func newPermutedGenerator(min, max, total int64, row uint32, seed int64) (*permutedGenerator, error) {
	var err error
	seq := apophenia.NewSequence(seed)
	g := &permutedGenerator{offset: min, total: total}
	g.permutation, err = apophenia.NewPermutation(max-min, row, seq)
	return g, err
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

// newLinearValueGenerator creates a new linearValueGenerator.
func newLinearValueGenerator(min, max, seed int64) (*linearValueGenerator, error) {
	g := &linearValueGenerator{offset: min, max: uint64(max) - uint64(min), seq: apophenia.NewSequence(seed)}
	g.bitoffset = apophenia.OffsetFor(apophenia.SequenceUser1, 0, 0, 0)
	return g, nil
}

func (g *linearValueGenerator) Nth(n int64) int64 {
	g.bitoffset.Lo = uint64(n)
	val := g.seq.BitsAt(g.bitoffset).Lo % g.max
	return int64(val) + g.offset
}

// zipfValueGenerator generator generates values with a Zipf distribution.
type zipfValueGenerator struct {
	z      *apophenia.Zipf
	offset int64
}

func newZipfValueGenerator(s, v float64, min, max, seed int64) (*zipfValueGenerator, error) {
	var err error
	g := zipfValueGenerator{offset: min}
	g.z, err = apophenia.NewZipf(s, v, uint64(max)-uint64(min), 0, apophenia.NewSequence(seed))
	if err != nil {
		return nil, err
	}
	return &g, nil
}

func (g *zipfValueGenerator) Nth(n int64) int64 {
	val := g.z.Nth(uint64(n))
	return int64(val) + g.offset
}

type permutedValueGenerator struct {
	base     valueGenerator
	permuter *apophenia.Permutation
	offset   int64
}

func newPermutedValueGenerator(base valueGenerator, min, max, seed int64) (*permutedValueGenerator, error) {
	var err error
	seq := apophenia.NewSequence(seed)
	g := permutedValueGenerator{base: base, offset: min}
	// 2 is an arbitrary magic number; we used 0 and 1 for other permutation sequences.
	g.permuter, err = apophenia.NewPermutation(max-min, 2, seq)
	return &g, err
}

func (g *permutedValueGenerator) Nth(n int64) int64 {
	val := g.base.Nth(n)
	val -= g.offset
	val = g.permuter.Nth(val)
	val += g.offset
	return val
}

type singleValueGenerator struct {
	colGen         sequenceGenerator
	valueGen       valueGenerator
	values         int64
	tries          int64
	density, scale uint64
	weighted       *apophenia.Weighted
	completed      bool
	updateChan     chan taskUpdate
	updateID       string
}

func (g *singleValueGenerator) Values() (int64, int64) {
	return g.values, g.tries
}

// Iterate loops over columns, producing a value for each column. If a density
// was specified, it returns only some of these values.
func (g *singleValueGenerator) Iterate() (col int64, value int64, done bool, ok bool) {
	for {
		col, done = g.colGen.Next()
		value = g.valueGen.Nth(col)
		g.tries++
		if g.updateChan != nil && (g.tries%10000) == 0 {
			cols, _ := g.colGen.Status()
			g.updateChan <- taskUpdate{id: g.updateID, colCount: cols, rowCount: 0, done: g.completed}
		}
		if g.weighted == nil {
			g.completed = done
			return col, value, done, true
		}
		offset := apophenia.OffsetFor(apophenia.SequenceWeighted, 0, 0, uint64(col))
		bit := g.weighted.Bit(offset, g.density, g.scale)
		if bit == 1 {
			g.completed = done
			return col, value, done, true
		}
		if done {
			return col, value, done, false
		}
	}
}

type fieldValueGenerator struct {
	singleValueGenerator
}

// NextRecord returns the next value pair from the fieldValueGenerator,
// as a pilosa.FieldValue.
func (g *fieldValueGenerator) NextRecord() (rec pilosa.Record, err error) {
	if g.completed {
		g.updateChan <- taskUpdate{id: g.updateID, colCount: g.tries, rowCount: 0, done: true}
		return nil, io.EOF
	}
	col, val, _, ok := g.Iterate()
	if !ok {
		return nil, io.EOF
	}
	g.values++
	return pilosa.FieldValue{ColumnID: uint64(col), Value: val}, nil
}

type columnValueGenerator struct {
	singleValueGenerator
}

// NextRecord returns the next value pair from the columnValueGenerator,
// as a pilosa.Column.
func (g *columnValueGenerator) NextRecord() (pilosa.Record, error) {
	if g.completed {
		return nil, io.EOF
	}
	col, val, _, ok := g.Iterate()
	if !ok {
		return nil, io.EOF
	}
	g.values++
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

func (g *zipfDensityGenerator) Density(col, row uint64) uint64 {
	// from the README as of when I wrote this:
	// For instance, with v=2, s=2, the k=0 probability is proportional to
	// `(2+0)**(-2)` (1/4), and the k=1 probability is proportional to
	// `(2+1)**(-2)` (1/9). Thus, the probability of a bit being set in the k=1 row is
	// 4/9 the base density.
	proportion := math.Pow(float64(row)+g.zipfV, -g.zipfS)
	return uint64(g.base * proportion * g.scale)
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
	g := maybeDensityGenerator{chance: uint64(float64(*fs.DensityScale) * *fs.Chance), scale: *fs.DensityScale}
	g.weighted, err = apophenia.NewWeighted(apophenia.NewSequence(seed))
	if err != nil {
		return nil
	}
	if fs.Next != nil {
		g.next, _ = makeDensityGenerator(fs.Next, seed)
	}
	g.generator = baseDensityGenerator(fs)
	return &g
}

func (g *maybeDensityGenerator) Density(col, row uint64) uint64 {
	if g == nil {
		return 0
	}
	// we ignore row here, because we want to get the same selection of density
	// for a given column every time.
	offset := apophenia.OffsetFor(apophenia.SequenceWeighted, 0, 0, uint64(col))
	bit := g.weighted.Bit(offset, g.chance, g.scale)
	if bit == 1 {
		return g.generator.Density(col, row)
	}
	if g.next != nil {
		return g.next.Density(col, row)
	}
	return 0
}

func baseDensityGenerator(fs *fieldSpec) densityGenerator {
	switch fs.ValueRule {
	case densityTypeLinear:
		g := fixedDensityGenerator(float64(*fs.DensityScale) * fs.Density)
		return &g
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
	updateChan       chan taskUpdate
	updateID         string
}

// Values yields the number of values generated, and also the number of
// positions evaluated.
func (g *doubleValueGenerator) Values() (int64, int64) {
	return g.values, g.tries
}

// rowMajorValueGenerator is a generator which generates values for every
// column for each row in turn. This is usually dramatically faster with
// Pilosa's server.
type rowMajorValueGenerator struct {
	doubleValueGenerator
}

// NextRecord finds the next record, if one is available.
func (g *rowMajorValueGenerator) NextRecord() (pilosa.Record, error) {
	for !g.colDone || !g.rowDone {
		if g.colDone {
			g.row, g.rowDone = g.rowGen.Next()
			if !g.densityPerCol {
				g.density = g.densityGen.Density(uint64(g.col), uint64(g.row))
			}
		}
		g.col, g.colDone = g.colGen.Next()
		if g.densityPerCol {
			g.density = g.densityGen.Density(uint64(g.col), uint64(g.row))
		}
		// use row as the "seed" for Weighted computations, so each row
		// can have different values.
		offset := apophenia.OffsetFor(apophenia.SequenceWeighted, uint32(g.row), 0, uint64(g.col))
		bit := g.weighted.Bit(offset, g.density, g.densityScale)
		g.tries++
		if g.updateChan != nil && g.tries%10000 == 0 {
			cols, _ := g.colGen.Status()
			rows, _ := g.rowGen.Status()
			g.updateChan <- taskUpdate{id: g.updateID, colCount: cols, rowCount: rows, done: false}
		}
		if bit != 0 {
			g.values++
			return pilosa.Column{ColumnID: uint64(g.col), RowID: uint64(g.row)}, nil
		}

	}
	if g.updateChan != nil {
		cols, _ := g.colGen.Status()
		rows, _ := g.rowGen.Status()
		g.updateChan <- taskUpdate{id: g.updateID, colCount: cols, rowCount: rows, done: true}
	}
	return nil, io.EOF
}

// columnMajorValueGenerator is a generator which generates every row value
// for each column in turn.
type columnMajorValueGenerator struct {
	doubleValueGenerator
}

// NextRecord returns the next record, if one is available.
func (g *columnMajorValueGenerator) NextRecord() (pilosa.Record, error) {
	for !g.colDone || !g.rowDone {
		if g.rowDone {
			g.col, g.colDone = g.colGen.Next()
		}
		g.row, g.rowDone = g.rowGen.Next()
		offset := apophenia.OffsetFor(apophenia.SequenceWeighted, uint32(g.row), 0, uint64(g.col))
		density := g.densityGen.Density(uint64(g.col), uint64(g.row))
		bit := g.weighted.Bit(offset, density, g.densityScale)
		g.tries++
		if g.updateChan != nil && g.tries%10000 == 0 {
			cols, _ := g.colGen.Status()
			rows, _ := g.rowGen.Status()
			g.updateChan <- taskUpdate{id: g.updateID, colCount: cols, rowCount: rows, done: false}
		}
		if bit != 0 {
			g.values++
			return pilosa.Column{ColumnID: uint64(g.col), RowID: uint64(g.row)}, nil
		}
	}
	if g.updateChan != nil {
		cols, _ := g.colGen.Status()
		rows, _ := g.rowGen.Status()
		g.updateChan <- taskUpdate{id: g.updateID, colCount: cols, rowCount: rows, done: true}
	}
	return nil, io.EOF
}
