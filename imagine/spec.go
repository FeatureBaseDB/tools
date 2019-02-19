package main

//go:generate enumer -type=fieldType -trimprefix=fieldType -transform=kebab -text -output enums_fieldtype.go
//go:generate enumer -type=densityType -trimprefix=densityType -text -transform=kebab -output enums_densitytype.go
//go:generate enumer -type=dimensionOrder -trimprefix=dimensionOrder -text -transform=kebab -output enums_dimensionorder.go
//go:generate enumer -type=valueOrder -trimprefix=valueOrder -text -transform=kebab -output enums_valueorder.go
//go:generate enumer -type=cacheType -trimprefix=cacheType -text -transform=kebab -output enums_cachetype.go

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
)

type fieldType int

const (
	fieldTypeUndef fieldType = iota
	fieldTypeBSI
	fieldTypeSet
	fieldTypeMutex
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
)

type cacheType int

const (
	cacheTypeDefault cacheType = iota
	cacheTypeNone
	cacheTypeLRU
)

type tomlSpec struct {
	PathName     string `toml:"-"` // don't set in spec, this will be set to the file name
	Prefix       string // overruled by config setting
	DensityScale uint64 // scale used for density. scale=8 means density will be 0, 1/8, 2/8, 3/8...
	Version      string
	Seed         int64 // default PRNG seed
	Indexes      map[string]*indexSpec
	Workloads    []*workloadSpec
}

type indexSpec struct {
	Parent      *tomlSpec `toml:"-"`
	Name        string    `toml:"-"`
	Description string    // for human-friendly descriptions
	FullName    string    `toml:"-"` // not actually intended to be user-set
	Columns     uint64    // total columns to create data for
	Fields      map[string]*fieldSpec
	ThreadCount int    `help:"threadCount to use for importers"`
	Seed        *int64 // default PRNG seed
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
	Name         string      `toml:"-"`
	Type         fieldType   // "set", "mutex", "bsi"
	ZipfV, ZipfS float64     // the V/S parameters of a Zipf distribution
	Min, Max     int64       // Allowable value range for a BSI field. Row range for set/mutex fields.
	SourceIndex  string      // SourceIndex's columns are used as value range for this field.
	Density      float64     // Base density to use in [0,1].
	ValueRule    densityType // which of several hypothetical density/value algorithms to use.
	DensityScale *uint64     // optional density scale

	// Only useful for set/mutex fields.
	Cache cacheType // "lru" or "none", default is lru for set/mutex
}

type namedWorkload struct {
	SpecName  string
	Workloads []*workloadSpec
}

// workloadSpec describes an overall workload consisting of sequential operations
type workloadSpec struct {
	Name        string
	Description string
	Batches     []*batchSpec
}

// batchSpec describes a set of tasks to happen in parallel
type batchSpec struct {
	Description string
	Tasks       []*taskSpec
}

// taskSpec describes a single task, which is populating some kind of data
// in some kind of field.
type taskSpec struct {
	field                 *fieldSpec // once things are built up, this gets pointed to the actual field spec
	Index                 string
	IndexFullName         string `toml:"-"`
	Field                 string
	Seed                  *int64         // PRNG seed to use.
	Columns               *uint64        // total column space (defaults to index's)
	ColumnOrder, RowOrder valueOrder     // linear or permuted orders (or stride, for columns)
	DimensionOrder        dimensionOrder // row-major/column-major. only meaningful for sets.
	Stride                uint64         // stride size when iterating on columns with columnOrder "stride"
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
	case fieldTypeBSI:
		return fmt.Sprintf("BSI: Min %d, Max %d, density %s", fs.Min, fs.Max, density)
	default:
		return fmt.Sprintf("%#v", *fs)
	}
}

func describeIndexes(spec *tomlSpec) {
	fmt.Printf("spec %s:\n", spec.PathName)
	for _, index := range spec.Indexes {
		describeIndex(index)
	}
}

func describeIndex(spec *indexSpec) {
	fmt.Printf("  index %s:\n", spec)
	if spec == nil {
		return
	}
	for key, f := range spec.Fields {
		fmt.Printf("    %s: %s\n", key, f)
	}
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

// Cleanup does additional validation and cleanup which may not be possible
// until the main program's filled in some blanks, such as a possible Prefix
// override specified on the command line.
func (ts *tomlSpec) Cleanup(conf *Config) error {
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
	is.FullName = is.Parent.Prefix + "-" + is.Name
	if is.Seed == nil {
		is.Seed = &is.Parent.Seed
	}
	if conf.ColumnScale != 0 {
		is.Columns *= uint64(conf.ColumnScale)
	}
	for name, field := range is.Fields {
		field.Parent = is
		field.Name = name
		if err := field.Cleanup(conf); err != nil {
			return fmt.Errorf("field %s/%s: %s", is.Name, name, err)
		}

	}
	if is.ThreadCount < 1 {
		is.ThreadCount = 1
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
	if is.ThreadCount != 0 {
		if other.ThreadCount != 0 && other.ThreadCount != is.ThreadCount {
			return fmt.Errorf("conflicting thread counts given for index '%s' [%d vs %d]", is.Name, is.ThreadCount, other.ThreadCount)
		}
	} else {
		is.ThreadCount = other.ThreadCount
	}
	if is.Seed != nil {
		if other.Seed != nil && *other.Seed != *is.Seed {
			return fmt.Errorf("conflicting seeds given for index '%s' [%d vs %d]", is.Name, *is.Seed, *other.Seed)
		}
	} else {
		is.Seed = other.Seed
	}
	for name, fs := range other.Fields {
		if is.Fields[name] != nil {
			return fmt.Errorf("field '%s' defined twice for index '%s'", name, is.Name)
		}
		is.Fields[name] = fs
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
	if fs.Cache == cacheTypeDefault {
		switch fs.Type {
		case fieldTypeSet, fieldTypeMutex:
			fs.Cache = cacheTypeLRU
		case fieldTypeBSI:
			fs.Cache = cacheTypeNone
		}
	}

	if fs.Type == fieldTypeBSI && fs.Cache != cacheTypeNone {
		return fmt.Errorf("field %s specifies a cache (%v) for a BSI field", fs.Name, fs.Cache)
	}
	return nil
}

func (ws *workloadSpec) Cleanup(conf *Config) error {
	for _, batch := range ws.Batches {
		err := batch.Cleanup(conf)
		if err != nil {
			return err
		}
	}
	return nil
}

func (bs *batchSpec) Cleanup(conf *Config) error {
	for _, task := range bs.Tasks {
		err := task.Cleanup(conf)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ts *taskSpec) Cleanup(conf *Config) error {
	index, ok := conf.indexes[ts.Index]
	if !ok {
		return fmt.Errorf("undefined index '%s' in task", ts.Index)
	}
	ts.IndexFullName = index.FullName
	field, ok := index.Fields[ts.Field]
	if !ok {
		return fmt.Errorf("undefined field '%s' in index '%s' in task", ts.Field, ts.Index)
	}
	ts.field = field
	if ts.Seed == nil {
		ts.Seed = ts.field.Parent.Seed
	}
	if ts.RowOrder == valueOrderStride {
		return fmt.Errorf("field %s: row order cannot be stride-based", ts.field.Name)
	}
	if ts.ColumnOrder == valueOrderStride && ts.Stride == 0 {
		return fmt.Errorf("field %s: stride size must be specified", ts.field.Name)
	}
	if ts.ColumnOrder != valueOrderStride && ts.Stride != 0 {
		return fmt.Errorf("field %s: stride size meaningless when not using stride column order", ts.field.Name)
	}
	// Having griped about Stride being set when inappropriate, we now set it
	// to 1 in those cases so we can always use the stride to compute
	// a value. Index N will use `(Stride*N) % range`.
	if ts.ColumnOrder != valueOrderStride {
		ts.Stride = 1
	}
	if ts.Columns == nil {
		ts.Columns = &ts.field.Parent.Columns
	} else {
		if conf.ColumnScale != 0 {
			*ts.Columns *= uint64(conf.ColumnScale)
		}
		if *ts.Columns > ts.field.Parent.Columns {
			return fmt.Errorf("field %s has %d columns specified, larger than index's %d", ts.field.Name,
				*ts.Columns, ts.field.Parent.Columns)
		}
	}
	if ts.DimensionOrder != dimensionOrderRow && ts.field.Type != fieldTypeSet {
		return fmt.Errorf("field %s: column-major dimension order is only supported for sets", ts.field.Name)
	}
	return nil
}
