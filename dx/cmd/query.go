package dx

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/spf13/cobra"
	// TODO: parse toml file to automatically get index, field, min, max values
	// "github.com/pelletier/go-toml"
	"github.com/pilosa/go-pilosa"
	// TODO: bench.Result is json, convert to non-JSON
	// for results, stats
	// "github.com/pilosa/tools/bench"
)

// NumQueries is the flag for the number of queries to run.
var NumQueries int

// CSIF is a shorthand for denoting the specific
// client, schema, index, and field we are interested in.
type CSIF struct {
	Client 	*pilosa.Client
	Schema 	*pilosa.Schema
	Index	*pilosa.Index
	Field	*pilosa.Field
}

// NewQueryCommand initializes a new ingest command for dx.
func NewQueryCommand() *cobra.Command {
	ingestCmd := &cobra.Command{
		Use:   "query",
		Short: "perform random queries",
		Long:  `Perform randomly generated queries against both instances of Pilosa`,
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("query up")

		},
	}
	ingestCmd.PersistentFlags().IntVarP(&NumQueries, "queries", "q", 10000, "Number of queries to run")
	return ingestCmd
}

func executeQueries() {

}

// TODO: add error checking, fix comments
func initializeCSIF(instanceName, indexName, fieldName string) *CSIF {
	// initialize clients
	client, err := initializeClient(CHosts, CPort)
	if err != nil {
		fmt.Printf("could not create %v client: %v", instanceName, err)
		return nil
	}

	// initalize schemas
	schema, err := client.Schema()
	if !schema.HasIndex(indexName) {
		fmt.Printf("could not find index %v in %v schema", indexName, instanceName)
		return nil
	}

	// initalize indexes and fields
	index := schema.Index(indexName)
	if !index.HasField(fieldName) {
		fmt.Printf("could not find field %v in index %v in %v schema", fieldName, indexName, instanceName)
		return nil
	}
	field := index.Field(fieldName)

	return &CSIF{
		Client: client,
		Schema: schema,
		Index:	index,
		Field:	field,
	}
}

func runQuery(max int, cCSIF, pCSIF *CSIF) {
	cTimeChan := make(chan int)
	cResultChan := make(chan pilosa.QueryResult)
	pTimeChan := make(chan int)
	pResultChan := make(chan pilosa.QueryResult)

	go runQueryOnInstance(cResultChan, cTimeChan, max, cCSIF)
	go runQueryOnInstance(pResultChan, pTimeChan, max, pCSIF)

	cTime := <- cTimeChan
	cResult := <- cResultChan
	pTime := <- pTimeChan
	pResult := <- pResultChan

	// TODO: analysis
	// TODO: printing
	// TODO: make struct to consolidate time
	fmt.Println(cTime, pTime, cResult, pResult)
}

func runQueryOnInstance(result chan pilosa.QueryResult, duration chan int, max int, csif *CSIF) {
	// TODO: check if int suffices
	client := csif.Client
	index := csif.Index
	field := csif.Field
	
	row1 := rand.Intn(max)
	row2 := rand.Intn(max)
	now := time.Now()
	rowQ := index.Intersect(field.Row(row1), field.Row(row2))
	response, _ := client.Query(rowQ)
	duration <- int(time.Since(now))
	result <- response.Result()
}
