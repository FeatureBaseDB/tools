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
	Indexes      []*indexSpec
	Prefix       string // overruled by config setting
	DensityScale uint64 // scale used for density. scale=8 means density will be 0, 1/8, 2/8, 3/8...
	Version      string
	Seed         int64 // default PRNG seed
}

type indexSpec struct {
	parent      *tomlSpec
	Name        string
	Description string // for human-friendly descriptions
	FullName    string // not actually intended to be user-set
	Columns     uint64 // total columns to create data for
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
	parent *indexSpec // the indexSpec this field applies to

	// common values for all the field types
	Name                  string
	Type                  fieldType   // "set", "mutex", "bsi"
	Columns               *uint64     // total column space (defaults to index's)
	ColumnOrder, RowOrder valueOrder  // linear or permuted orders (or stride, for columns)
	ZipfV, ZipfS          float64     // the V/S parameters of a Zipf distribution
	Seed                  *int64      // PRNG seed to use.
	Min, Max              int64       // Allowable value range for a BSI field. Row range for set/mutex fields.
	SourceIndex           string      // SourceIndex's columns are used as value range for this field.
	Density               float64     // Base density to use in [0,1].
	ValueRule             densityType // which of several hypothetical density/value algorithms to use.
	DensityScale          *uint64     // optional density scale
	Stride                uint64      // stride size when iterating on columns with columnOrder "stride"

	// Only useful for set/mutex fields.
	Cache          cacheType      // "lru" or "none", default is lru for set/mutex
	DimensionOrder dimensionOrder // row-major/column-major. only meaningful for sets.
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
	for _, index := range spec.Indexes {
		describeIndex(index)
	}
}

func describeIndex(spec *indexSpec) {
	fmt.Printf("index %s:\n", spec)
	if spec == nil {
		return
	}
	for key, f := range spec.Fields {
		fmt.Printf("  %s: %s\n", key, f)
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
func (ts *tomlSpec) Cleanup() error {
	fixDensityScale(&ts.DensityScale)
	// Copy in column counts; all fields have an innate column count equal
	// to the index's column count.
	for _, indexSpec := range ts.Indexes {
		indexSpec.parent = ts
		if err := indexSpec.Cleanup(); err != nil {
			return fmt.Errorf("error in spec: %s", err)
		}

	}
	return nil
}

// Cleanup does data validation and cleanup for an indexSpec.
func (is *indexSpec) Cleanup() error {
	if is.Name == "" {
		return errors.New("index has no specified name")
	}
	if is.FullName != "" {
		return fmt.Errorf("index full name must not be specified, it's computed from prefix ['%s']", is.FullName)
	}
	is.FullName = is.parent.Prefix + "-" + is.Name
	if is.Seed == nil {
		is.Seed = &is.parent.Seed
	}
	for name, field := range is.Fields {
		field.parent = is
		if field.Name != "" {
			return fmt.Errorf("field name must not be specified, use the map key [%s/%s]", is.Name, name)
		}
		field.Name = name
		if err := field.Cleanup(); err != nil {
			return fmt.Errorf("field %s/%s: %s", is.Name, name, err)
		}

	}
	if is.ThreadCount < 1 {
		is.ThreadCount = 1
	}
	return nil
}

// Cleanup does data validation and checking for a fieldSpec.
func (fs *fieldSpec) Cleanup() error {
	if fs.Seed == nil {
		fs.Seed = fs.parent.Seed
	}
	if fs.DensityScale == nil {
		// inherit parent's scale
		fs.DensityScale = &fs.parent.parent.DensityScale
	} else {
		fixDensityScale(fs.DensityScale)
	}

	if fs.RowOrder == valueOrderStride {
		return fmt.Errorf("field %s: row order cannot be stride-based", fs.Name)
	}
	if fs.ColumnOrder == valueOrderStride && fs.Stride == 0 {
		return fmt.Errorf("field %s: stride size must be specified", fs.Name)
	}
	if fs.ColumnOrder != valueOrderStride && fs.Stride != 0 {
		return fmt.Errorf("field %s: stride size meaningless when not using stride column order", fs.Name)
	}
	// Having griped about Stride being set when inappropriate, we now set it
	// to 1 in those cases so we can always use the stride to compute
	// a value. Index N will use `(Stride*N) % range`.
	if fs.ColumnOrder != valueOrderStride {
		fs.Stride = 1
	}
	if fs.Columns == nil {
		if fs.ColumnOrder == valueOrderStride {
			// compute a number of columns from the stride
			col := fs.parent.Columns / fs.Stride
			fs.Columns = &col
		} else {
			fs.Columns = &fs.parent.Columns
		}
	} else {
		if *fs.Columns > fs.parent.Columns {
			return fmt.Errorf("field %s has %d columns specified, larger than index's %d", fs.Name,
				*fs.Columns, fs.parent.Columns)
		}
	}
	if fs.SourceIndex != "" {
		if fs.Min != 0 || fs.Max != 0 {
			return fmt.Errorf("field %s specifies both min/max (%d/%d) and source index (%s)",
				fs.Name, fs.Min, fs.Max, fs.SourceIndex)
		}
		found := false
		for _, is := range fs.parent.parent.Indexes {
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
	if fs.DimensionOrder != dimensionOrderRow && fs.Type != fieldTypeSet {
		return fmt.Errorf("field %s: column-major dimension order is only supported for sets", fs.Name)
	}
	if fs.Type == fieldTypeBSI && fs.Cache != cacheTypeNone {
		return fmt.Errorf("field %s specifies a cache (%v) for a BSI field", fs.Name, fs.Cache)
	}
	return nil
}
