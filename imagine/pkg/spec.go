package imagine

//go:generate enumer -type=fieldType -trimprefix=fieldType -transform=kebab -text -output enums_fieldtype.go
//go:generate enumer -type=densityType -trimprefix=densityType -text -transform=kebab -output enums_densitytype.go
//go:generate enumer -type=dimensionOrder -trimprefix=dimensionOrder -text -transform=kebab -output enums_dimensionorder.go
//go:generate enumer -type=valueOrder -trimprefix=valueOrder -text -transform=kebab -output enums_valueorder.go
//go:generate enumer -type=cacheType -trimprefix=cacheType -text -transform=kebab -output enums_cachetype.go
//go:generate enumer -type=stampType -trimprefix=stampType -text -transform=kebab -output enums_stamptype.go
//go:generate enumer -type=timeQuantum -trimprefix=timeQuantum -text -transform=caps -output enums_timequantum.go

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

type fieldType int

const (
	fieldTypeUndef fieldType = iota
	fieldTypeInt
	fieldTypeSet
	fieldTypeMutex
	fieldTypeTime
)

type densityType int

const (
	densityTypeLinear densityType = iota
	densityTypeZipf
)

type dimensionOrder int

const (
	dimensionOrderRow dimensionOrder = iota
	dimensionOrderColumn
)

type valueOrder int

const (
	valueOrderLinear valueOrder = iota
	valueOrderStride
	valueOrderPermute
	valueOrderZipf
)

type cacheType int

const (
	cacheTypeDefault cacheType = iota
	cacheTypeNone
	cacheTypeLRU
	cacheTypeRanked
)

type stampType int

const (
	stampTypeNone stampType = iota
	stampTypeIncreasing
	stampTypeRandom
)

type timeQuantum int

const (
	timeQuantumY timeQuantum = iota
	timeQuantumYM
	timeQuantumYMD
	timeQuantumYMDH
)

type columnOffset int64

func (c *columnOffset) UnmarshalText(input []byte) error {
	in := string(input)
	if in == "append" {
		*c = -1
		return nil
	}
	val, err := strconv.ParseInt(in, 10, 64)
	if err != nil {
		return err
	}
	*c = columnOffset(val)
	return nil
}

type duration time.Duration

func (d *duration) UnmarshalText(input []byte) error {
	dur, err := time.ParseDuration(string(input))
	if err != nil {
		return err
	}
	*d = duration(dur)
	return nil
}

type tomlSpec struct {
	PathName     string `toml:"-"` // don't set in spec, this will be set to the file name
	Prefix       string // overruled by config setting
	DensityScale uint64 // scale used for density. scale=8 means density will be 0, 1/8, 2/8, 3/8...
	Version      string
	Seed         int64 // default PRNG seed
	Indexes      map[string]*indexSpec
	Workloads    []*workloadSpec
	FastSparse   bool
	CachePath    string // the path for random uint cache
}

type indexSpec struct {
	Parent        *tomlSpec             `toml:"-"`
	Name          string                `toml:"-"`
	Description   string                // for human-friendly descriptions
	FullName      string                `toml:"-"` // not actually intended to be user-set
	Columns       uint64                // total columns to create data for
	UniqueColumns uint64                // number of random columns to create when fastSparse=true
	FieldsByName  map[string]*fieldSpec `toml:"-"`
	Fields        []*fieldSpec
	Seed          *int64 // default PRNG seed
	ShardWidth    uint64
}

func (is *indexSpec) String() string {
	if is == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s [%s], %d columns, %d fields", is.Name, is.FullName, is.Columns, len(is.Fields))
}

// fieldSpec describes a given field within an index.
type fieldSpec struct {
	// internals
	Parent *indexSpec `toml:"-"` // the indexSpec this field applies to

	// common values for all the field types
	Name          string
	Type          fieldType    // "set", "mutex", "int"
	ZipfV, ZipfS  float64      // the V/S parameters of a Zipf distribution
	ZipfA         float64      // alpha parameter for a zipf distribution, should be >= 0
	Min, Max      int64        // Allowable value range for an int field. Row range for set/mutex fields.
	SourceIndex   string       // SourceIndex's columns are used as value range for this field.
	Density       float64      // Base density to use in [0,1].
	ValueRule     densityType  // which of several hypothetical density/value algorithms to use.
	DensityScale  *uint64      // optional density scale
	Chance        *float64     // probability of using this fieldSpec for a given column
	Next          *fieldSpec   `toml:"-"` // next fieldspec to try
	HighestColumn int64        `toml:"-"` // highest column we've generated for this field
	Quantum       *timeQuantum // time quantum, useful only for time fields
	FastSparse    bool
	CachePath     string

	// Only useful for set/mutex fields.
	Cache     cacheType // "ranked", "lru", or "none", default is ranked for set/mutex
	CacheSize int
}

type namedWorkload struct {
	SpecName  string
	Workloads []*workloadSpec
}

// workloadSpec describes an overall workload consisting of sequential operations
type workloadSpec struct {
	Name        string
	Description string
	Tasks       []*taskSpec
	ThreadCount *int // threads to use for each importer
	BatchSize   *int
	UseRoaring  *bool // configure go-pilosa to use Pilosa's import-roaring endpoint
}

// taskSpec describes a single task, which is populating some kind of data
// in some kind of field.
type taskSpec struct {
	Parent                *workloadSpec `toml:"-"`
	FieldSpec             *fieldSpec    `toml:"-"` // once things are built up, this gets pointed to the actual field spec
	Index                 string
	IndexFullName         string `toml:"-"`
	Field                 string
	Seed                  *int64         // PRNG seed to use.
	Columns               *uint64        // total column space (defaults to index's)
	ColumnOrder, RowOrder valueOrder     // linear or permuted orders (or stride, for columns)
	ColumnOffset          columnOffset   // starting column or "append"
	Stamp                 stampType      // what timestamps if any to use
	StampRange            *duration      // interval to space stamps over
	StampStart            *time.Time     // starting point for stamps
	DimensionOrder        dimensionOrder // row-major/column-major. only meaningful for sets.
	Stride                uint64         // stride size when iterating on columns with columnOrder "stride"
	BatchSize             *int           // override batch batchsize
	ZipfV, ZipfS          float64
	ZipfRange             *uint64
	UseRoaring            *bool // configure go-pilosa to use Pilosa's import-roaring endpoint
}

func (fs *fieldSpec) String() string {
	if fs == nil {
		return "<nil>"
	}
	var density string
	switch fs.ValueRule {
	default:
		density = fmt.Sprintf("?%s?", fs.ValueRule)
	case densityTypeLinear:
		density = fmt.Sprintf("%.3f", fs.Density)
	case densityTypeZipf:
		density = fmt.Sprintf("%.3f base, Zipf v %.3f s %.3f", fs.Density, fs.ZipfV, fs.ZipfS)
	}
	switch fs.Type {
	case fieldTypeSet:
		return fmt.Sprintf("set: rows %d, density %s", fs.Max, density)
	case fieldTypeInt:
		return fmt.Sprintf("int: Min %d, Max %d, density %s", fs.Min, fs.Max, density)
	default:
		return fmt.Sprintf("%#v", *fs)
	}
}

func describeSpec(spec *tomlSpec) {
	fmt.Printf("spec %s:\n", spec.PathName)
	fmt.Printf(" indexes:\n")
	for _, index := range spec.Indexes {
		describeIndex(index)
	}
	fmt.Printf(" workloads:\n")
	for _, workload := range spec.Workloads {
		describeWorkload(workload)
	}
}

func describeIndex(spec *indexSpec) {
	fmt.Printf("  index %s:\n", spec)
	if spec == nil {
		return
	}
	for _, f := range spec.FieldsByName {
		describeField(f, true)
		for f.Next != nil {
			f = f.Next
			describeField(f, false)
		}
	}
}

func describeField(spec *fieldSpec, showName bool) {
	if showName {
		fmt.Printf("    %s: ", spec.Name)
	} else {
		fmt.Printf("    %*s: ", len(spec.Name), "")
	}
	fmt.Printf(" [%.2f] %s\n", *spec.Chance, spec)
}

func describeWorkload(wl *workloadSpec) {
	if wl == nil {
		fmt.Printf("  nil workload\n")
		return
	}
	fmt.Printf("  workload %s:\n", wl.Name)
	for _, t := range wl.Tasks {
		fmt.Printf("    task %v\n", t)
	}
}

func (ts *taskSpec) String() string {
	offset := ""
	if ts.ColumnOffset != 0 {
		offset = fmt.Sprintf(" starting at %d", ts.ColumnOffset)
	}
	return fmt.Sprintf("%s/%s: %d columns%s", ts.Index, ts.Field, *ts.Columns, offset)
}

func readSpec(path string) (*tomlSpec, error) {
	var ts tomlSpec
	md, err := toml.DecodeFile(path, &ts)
	if err != nil {
		return nil, err
	}
	// don't allow keys we haven't heard of
	undecodedKeys := md.Undecoded()
	if len(undecodedKeys) > 0 {
		keyNames := make([]string, len(undecodedKeys))
		for idx, key := range undecodedKeys {
			keyNames[idx] = strings.Join(key, ".")
		}
		return nil, fmt.Errorf("undecoded keys: %s", strings.Join(keyNames, ", "))
	}
	// version checking. this may some day have more to do.
	if ts.Version != "1.0" {
		if ts.Version != "" {
			return nil, fmt.Errorf("version must be specified as '1.0' (got '%s')", ts.Version)
		}
		return nil, errors.New("version must be specified as '1.0'")

	}
	if ts.PathName != "" {
		return nil, errors.New("path name must not be specified in the spec")
	}
	ts.PathName = path
	return &ts, nil
}

// helper function: used for both the global default scale and per-field
// settings.
func fixDensityScale(densityScale *uint64) {
	if densityScale == nil {
		return
	}
	// you can't specify density scale under 2.
	if *densityScale < 2 {
		fmt.Fprintf(os.Stderr, "warning: forcing density scale to 2 (minimum is 2)\n")
		*densityScale = 2
	}
	if *densityScale > (1 << 31) {
		fmt.Fprintf(os.Stderr, "warning: forcing density scale to 1<<31 (maximum is 1<<31)\n")
		*densityScale = 1 << 31
	}
	if *densityScale&(*densityScale-1) != 0 {
		u := *densityScale
		u--
		u |= u >> 1
		u |= u >> 2
		u |= u >> 3
		u |= u >> 4
		u |= u >> 5
		u++
		fmt.Fprintf(os.Stderr, "warning: forcing density scale to %d, next power of 2 above %d\n", u, *densityScale)
		*densityScale = u
	}
}

// CleanupIndexes does additional validation and cleanup which may not be
// possible until the main program's filled in some blanks, such as a possible
// Prefix override specified on the command line.
func (ts *tomlSpec) CleanupIndexes(conf *Config) error {
	fixDensityScale(&ts.DensityScale)
	// Copy in column counts; all fields have an innate column count equal
	// to the index's column count.
	for name, indexSpec := range ts.Indexes {
		indexSpec.Parent = ts
		indexSpec.Name = name
		if err := indexSpec.Cleanup(conf); err != nil {
			return fmt.Errorf("error in spec: %s", err)
		}
		if conf.indexes[name] == nil {
			conf.indexes[name] = indexSpec
			continue
		}
		// we have to try to merge a thing
		err := conf.indexes[name].Merge(indexSpec)
		if err != nil {
			return err
		}
	}
	return nil
}

// CleanupWorkloads does workload cleanup. This has to be done in a
// separate pass if you want workloads to be able to refer to indexes
// in files not yet read.
func (ts *tomlSpec) CleanupWorkloads(conf *Config) error {
	for _, workload := range ts.Workloads {
		err := workload.Cleanup(conf)
		if err != nil {
			return err
		}
	}
	wl := namedWorkload{
		SpecName:  ts.PathName,
		Workloads: ts.Workloads,
	}
	conf.workloads = append(conf.workloads, wl)
	return nil
}

// Cleanup does data validation and cleanup for an indexSpec.
func (is *indexSpec) Cleanup(conf *Config) error {
	is.FullName = is.Parent.Prefix + is.Name
	if is.FieldsByName == nil {
		is.FieldsByName = make(map[string]*fieldSpec, len(is.Fields))
	}
	if is.Seed == nil {
		is.Seed = &is.Parent.Seed
	}
	if conf.ColumnScale != 0 {
		is.Columns *= uint64(conf.ColumnScale)
	}
	for _, field := range is.Fields {
		field.Parent = is
		if is.FieldsByName[field.Name] != nil {
			if is.FieldsByName[field.Name].Type != field.Type {
				return fmt.Errorf("field %s/%s: incompatible type specifiers %v and %v\n", is.Name, field.Name,
					is.FieldsByName[field.Name].Type, field.Type)
			}
			if is.FieldsByName[field.Name].Max != field.Max {
				if field.Max != 0 {
					return fmt.Errorf("field %s/%s: incompatible max specifiers %d and %d\n", is.Name, field.Name,
						is.FieldsByName[field.Name].Max, field.Max)
				}
				field.Max = is.FieldsByName[field.Name].Max
			}
			// push this in front of the previous one
			field.Next = is.FieldsByName[field.Name]
		}
		is.FieldsByName[field.Name] = field
		if err := field.Cleanup(conf); err != nil {
			return fmt.Errorf("field %s/%s: %s", is.Name, field.Name, err)
		}
	}
	return nil
}

// Merge tries to merge a new spec into an existing one,
// adding new fields, combining non-field data, and erroring
// out on mismatches.
func (is *indexSpec) Merge(other *indexSpec) error {
	if is.Name != other.Name {
		return fmt.Errorf("impossibly, indexes '%s' and '%s' have the same name but have different names.",
			is.Name, other.Name)
	}
	if is.Description != "" {
		if other.Description != "" && other.Description != is.Description {
			return fmt.Errorf("conflicting descriptions given for index '%s'", is.Name)
		}
	} else {
		// merge it in
		is.Description = other.Description
	}
	// ignore FullName
	if is.Columns != 0 {
		if other.Columns != 0 && other.Columns != is.Columns {
			return fmt.Errorf("conflicting column counts given for index '%s' [%d vs %d]", is.Name, is.Columns, other.Columns)
		}
	} else {
		is.Columns = other.Columns
	}
	if is.Seed != nil {
		if other.Seed != nil && *other.Seed != *is.Seed {
			return fmt.Errorf("conflicting seeds given for index '%s' [%d vs %d]", is.Name, *is.Seed, *other.Seed)
		}
	} else {
		is.Seed = other.Seed
	}
	for name, fs := range other.FieldsByName {
		// prepend the new spec to the existing one
		if is.FieldsByName[name] != nil {
			for fs.Next != nil {
				fs = fs.Next
			}
			fs.Next = is.FieldsByName[name]
		}
		is.FieldsByName[name] = fs
	}
	return nil
}

// Cleanup does data validation and checking for a fieldSpec.
func (fs *fieldSpec) Cleanup(conf *Config) error {
	if fs.DensityScale == nil {
		// inherit parent's scale
		fs.DensityScale = &fs.Parent.Parent.DensityScale
	} else {
		fixDensityScale(fs.DensityScale)
	}
	fs.CachePath = fs.Parent.Parent.CachePath
	// no specified chance = 1.0
	if fs.Chance == nil {
		f := float64(1.0)
		fs.Chance = &f
	} else {
		if *fs.Chance < 0 || *fs.Chance > 1.0 {
			return fmt.Errorf("invalid chance %f (must be in [0,1])", *fs.Chance)
		}
	}
	if fs.SourceIndex != "" {
		if fs.Min != 0 || fs.Max != 0 {
			return fmt.Errorf("field %s specifies both min/max (%d/%d) and source index (%s)",
				fs.Name, fs.Min, fs.Max, fs.SourceIndex)
		}
		found := false
		for _, is := range fs.Parent.Parent.Indexes {
			if is.Name == fs.SourceIndex {
				found = true
				// note, Columns is technically uint64. don't specify > 2^63 columns.
				fs.Min = 0
				fs.Max = int64(is.Columns) - 1
				break
			}
		}
		if !found {
			return fmt.Errorf("field %s specifies source index '%s' which does not exist",
				fs.Name, fs.SourceIndex)
		}
	} else {
		// RowScale does not apply to SourceIndex
		if conf.RowScale != 0 {
			fs.Min *= conf.RowScale
			fs.Max *= conf.RowScale
		}
	}
	if fs.Max < fs.Min {
		return fmt.Errorf("field %s has maximum %d, less than minimum %d", fs.Name, fs.Max, fs.Min)
	}

	// I don't think there's any use for a CacheSize of 0 - just set CacheTypeNone in that case.
	if fs.CacheSize == 0 {
		fs.CacheSize = 1000
	}

	if fs.Cache == cacheTypeDefault {
		switch fs.Type {
		case fieldTypeSet, fieldTypeMutex:
			fs.Cache = cacheTypeRanked
		case fieldTypeInt:
			fs.Cache = cacheTypeNone
		}
	}
	if fs.Type == fieldTypeInt && fs.Cache != cacheTypeNone {
		return fmt.Errorf("field %s specifies a cache (%v) for an int field", fs.Name, fs.Cache)
	}
	if fs.Type == fieldTypeTime {
		if fs.Quantum == nil {
			q := timeQuantumYMDH
			fs.Quantum = &q
		}
	} else {
		if fs.Quantum != nil {
			return fmt.Errorf("field %s specifies a time quantum but is not a time field", fs.Name)
		}
	}
	return nil
}

// Cleanup performs bookkeeping tasks and error-checking, and calls the
// Cleanup method of associated tasks.
func (ws *workloadSpec) Cleanup(conf *Config) error {
	if conf.ThreadCount != 0 {
		ws.ThreadCount = &conf.ThreadCount
	} else if ws.ThreadCount != nil {
		if *ws.ThreadCount < 1 {
			return fmt.Errorf("invalid thread count %d [must be a positive number]", *ws.ThreadCount)
		}
	}
	for _, task := range ws.Tasks {
		task.Parent = ws
		err := task.Cleanup(conf)
		if err != nil {
			return err
		}
	}
	return nil
}

// Cleanup performs bookkeeping tasks, associates a taskSpec with the
// corresponding fieldSpec, propagates values for parameters with default
// values, and checks for spec errors.
func (ts *taskSpec) Cleanup(conf *Config) error {
	index, ok := conf.indexes[ts.Index]
	if !ok {
		return fmt.Errorf("undefined index '%s' in task", ts.Index)
	}
	ts.IndexFullName = index.FullName
	field, ok := index.FieldsByName[ts.Field]
	if !ok {
		return fmt.Errorf("undefined field '%s' in index '%s' in task", ts.Field, ts.Index)
	}
	ts.FieldSpec = field
	if ts.Seed == nil {
		ts.Seed = ts.FieldSpec.Parent.Seed
	}
	if ts.RowOrder == valueOrderStride {
		return fmt.Errorf("field %s: row order cannot be stride-based", ts.Field)
	}
	if ts.ColumnOrder == valueOrderStride && ts.Stride == 0 {
		return fmt.Errorf("field %s: stride size must be specified", ts.Field)
	}
	if ts.ColumnOrder != valueOrderStride && ts.Stride != 0 {
		return fmt.Errorf("field %s: stride size meaningless when not using stride column order", ts.Field)
	}
	if ts.RowOrder == valueOrderZipf {
		return fmt.Errorf("field %s: zipf is only valid as a column order for append operations", ts.Field)
	}

	// Having griped about Stride being set when inappropriate, we now set it
	// to 1 in those cases so we can always use the stride to compute
	// a value. Index N will use `(Stride*N) % range`.
	if ts.ColumnOrder != valueOrderStride {
		ts.Stride = 1
	}
	if ts.Columns == nil {
		ts.Columns = &ts.FieldSpec.Parent.Columns
	} else {
		if conf.ColumnScale != 0 {
			*ts.Columns *= uint64(conf.ColumnScale)
		}
	}
	if ts.ColumnOrder != valueOrderZipf {
		if ts.ZipfV != 0 || ts.ZipfS != 0 || ts.ZipfRange != nil {
			return fmt.Errorf("field %s: zipf values only available with zipf column order (only available with append)", ts.Field)
		}
	} else {
		if ts.ColumnOffset != -1 {
			return fmt.Errorf("field %s: zipf column order only available for append operations", ts.Field)
		}
		if ts.ZipfV < 1.0 || ts.ZipfS <= 1.0 {
			return fmt.Errorf("field %s: zipf column order requires V >= 1, S > 1", ts.Field)
		}
		if ts.ZipfRange == nil {
			ts.ZipfRange = ts.Columns
		}
	}
	if ts.DimensionOrder != dimensionOrderRow && ts.FieldSpec.Type != fieldTypeSet {
		return fmt.Errorf("field %s: column-major dimension order is only supported for sets", ts.Field)
	}
	if ts.BatchSize == nil {
		ts.BatchSize = ts.Parent.BatchSize
	}
	if ts.UseRoaring == nil {
		ts.UseRoaring = ts.Parent.UseRoaring
	}
	// handle timestamp behavior, if requested.
	if ts.Stamp == stampTypeNone {
		return nil
	}
	// We can't do timestamps with FieldValue returns.
	if ts.FieldSpec.Type == fieldTypeInt {
		return fmt.Errorf("field %s: Int fields don't support timestamps", ts.Field)
	}
	// default to one week
	if ts.StampRange == nil {
		week := duration(time.Hour * 7 * 24)
		ts.StampRange = &week
	}
	// default to range from now to StampRange ago
	if ts.StampStart == nil {
		start := time.Now().Add(-1 * time.Duration(*ts.StampRange))
		ts.StampStart = &start
	}
	return nil
}
