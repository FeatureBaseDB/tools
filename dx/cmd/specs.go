package dx

// import (
// 	"fmt"
// 	"github.com/pelletier/go-toml"
// )

// // GetSpecsInfo decodes the field name, min, and max from a specs file with the index "index".
// func GetSpecsInfo(specsFile string) (indexName string, fieldName string, fieldMin int64, fieldMax int64, err error) {
// 	config, err := toml.LoadFile(m.SpecsFile)
// 	if err != nil {
// 		return "", "", 0, 0, fmt.Errorf("could not load specs file: %v", err)
// 	}
// 	defer func() (string, string, int64, int64, error) {
// 		if err := recover(); err != nil {
// 			return "", "", 0, 0, fmt.Errorf("error finding fields in spec file: %v", err)
// 		}
// 		return indexName, fieldName, fieldMin, fieldMax, nil
// 	}()
	
// 	indexName = "index"
// 	fieldConfig := config.Get("indexes.users.fields").(*toml.Tree)
// 	fieldName = fieldConfig.Get("name").(string)
// 	fieldMin = fieldConfig.Get("min").(int64)
// 	fieldMax = fieldConfig.Get("max").(int64)
// 	return indexName, fieldName, fieldMin, fieldMax, nil
// }