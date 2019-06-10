package dx

import (
	"fmt"
	"github.com/pelletier/go-toml"
)

// specsConfig represents the parts of a specs file
// we are interested in for querying and printing.
type specsConfig struct {
	indexName string
	fieldName string
	min       int64
	max       int64
	columns   int64
}

// getSpecs automatically parses the specsFile
// and returns a default value if that fails.
func getSpecs(specsFile string) (*specsConfig, error) {
	indexName, fieldName, fieldMin, fieldMax, columns, err := getSpecsInfo(specsFile)
	if err != nil {
		m.Logger.Printf("could not parse specs file: %v, using default values instead", err)
		return &specsConfig{
			indexName: m.Prefix + "index",
			fieldName: "field",
			min:       0,
			max:       100000,
			columns:   1000000,
		}, fmt.Errorf("could not parse specs: %v", err)
	}
	return &specsConfig{
		indexName: m.Prefix + indexName,
		fieldName: fieldName,
		min:       fieldMin,
		max:       fieldMax,
		columns:   columns,
	}, nil
}

// getSpecsInfo decodes the field name, min, max, and number of columns
// from a specs file with the index name "index".
func getSpecsInfo(specsFile string) (indexName string, fieldName string, fieldMin int64, fieldMax int64, columns int64, err error) {
	config, err := toml.LoadFile(specsFile)
	if err != nil {
		return "", "", 0, 0, 0, fmt.Errorf("could not load specs file: %v", err)
	}
	defer func() {
		if err := recover(); err != nil {
			err = fmt.Errorf("error finding fields in spec file: %v", err)
		}
	}()

	indexName = "index"
	columns = config.Get("indexes." + indexName + ".columns").(int64)
	fieldConfig := config.Get("indexes." + indexName + ".fields").([]*toml.Tree)[0]
	fieldName = fieldConfig.Get("name").(string)
	fieldMin = fieldConfig.Get("min").(int64)
	fieldMax = fieldConfig.Get("max").(int64)
	return indexName, fieldName, fieldMin, fieldMax, columns, nil
}
