package dx

import (
	"fmt"

	"github.com/pilosa/go-pilosa"
	imagine "github.com/pilosa/tools/imagine/pkg"
)

type indexConfig struct {
	name    string
	index   *pilosa.Index
	fields  map[string]*fieldConfig
	columns uint64
}

func newIndexConfig(name string, columns uint64) *indexConfig {
	return &indexConfig{
		name:    name,
		fields:  make(map[string]*fieldConfig),
		columns: columns,
	}
}

func (iconf *indexConfig) deepcopy() *indexConfig {
	newIconf := newIndexConfig(iconf.name, iconf.columns)
	for fieldName, fconf := range iconf.fields {
		newIconf.fields[fieldName] = &fieldConfig{
			name: fconf.name,
			min:  fconf.min,
			max:  fconf.max,
		}
	}
	return newIconf
}

func deepcopy(iconfs map[string]*indexConfig) map[string]*indexConfig {
	newIconfs := make(map[string]*indexConfig, len(iconfs))

	for indexName, iconf := range iconfs {
		newIconfs[indexName] = iconf.deepcopy()
	}
	return newIconfs
}

type fieldConfig struct {
	name     string
	field    *pilosa.Field
	min, max int64
}

// TODO: validation for duplicate indexes
// TODO: suppport multiple specsFiles
func getSpecs(specsFile string) (map[string]*indexConfig, error) {
	tomlSpec, err := imagine.ReadSpec(specsFile)
	if err != nil {
		m.Logger.Printf("could not parse specs file: %v", err)
		return nil, fmt.Errorf("could not parse specs file: %v", err)
	}
	configs := make(map[string]*indexConfig)
	// add each index and field
	for indexName, indexSpec := range tomlSpec.Indexes {
		prefixedName := m.Prefix + indexName
		iconf := newIndexConfig(prefixedName, indexSpec.Columns)
		iconf.fields = make(map[string]*fieldConfig)
		for _, fieldSpec := range indexSpec.Fields {
			fconf := &fieldConfig{
				name: fieldSpec.Name,
				min:  fieldSpec.Min,
				max:  fieldSpec.Max,
			}
			iconf.fields[fieldSpec.Name] = fconf
		}
		configs[iconf.name] = iconf
	}
	return configs, nil
}
