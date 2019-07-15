package dx

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/pilosa/go-pilosa"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

// NewQueryCommand initializes a query command.
func NewQueryCommand(m *Main) *cobra.Command {
	queryCmd := &cobra.Command{
		Use:   "query",
		Short: "query the cluster/s",
		Long:  `Perform queries on the cluster/s.`,
		Run: func(cmd *cobra.Command, args []string) {

			if err := ExecuteQueries(m); err != nil {
				fmt.Printf("%+v", err)
				os.Exit(1)
			}

		},
	}

	queryCmd.PersistentFlags().Int64VarP(&m.NumQueries, "queries", "q", 100, "Number of queries to run")
	queryCmd.PersistentFlags().Int64VarP(&m.NumRows, "rows", "r", 2, "Number of rows to perform a query on")
	queryCmd.PersistentFlags().StringSliceVarP(&m.Indexes, "indexes", "i", nil, "Indexes to run queries on")
	queryCmd.PersistentFlags().BoolVarP(&m.ActualResults, "actualresults", "a", false, "Compare actual results of queries instead of counts")
	queryCmd.PersistentFlags().StringVar(&m.QueryTemplate, "querytemplate", "", "Use the previous result as query template")

	return queryCmd
}

// Query contains the information related to a single query.
type Query struct {
	ID          int64             `json:"id"`
	Type        queryType         `json:"query"`
	IndexName   string            `json:"index"`
	FieldName   string            `json:"field"`
	Rows        []int64           `json:"rows"`
	Time        TimeDuration      `json:"time"`
	Result      *pilosa.RowResult `json:"result,omitempty"`
	ResultCount *int64            `json:"resultcount,omitempty"`
	Error       error             `json:"-"`
}

// ExecuteQueries executes queries on the cluster/s.
func ExecuteQueries(m *Main) error {
	path, err := makeFolder(cmdQuery, m.DataDir)
	if err != nil {
		return errors.Wrap(err, "error creating folder for query results")
	}

	// use first cluster to determine indexes and fields
	clients, err := initializeClients(m.Hosts)
	if err != nil {
		return errors.Wrap(err, "error initializing client for first cluster")
	}
	indexSpec, err := defaultIndexSpecFromIndexes(clients[0], m.Indexes)
	if err != nil {
		return errors.Wrap(err, "error getting index spec from first cluster")
	}

	// queryChan is where the generated queries are passed into
	queryChan := make(chan Query)
	// qResultChans has a channel per client that takes the results of each query from queryChan on that client
	qResultChans := make([]chan Query, 0, len(clients))
	for i := 0; i < len(clients); i++ {
		qResultChannel := make(chan Query)
		qResultChans = append(qResultChans, qResultChannel)
	}

	now := time.Now()

	// make queries
	if m.QueryTemplate == "" {
		go populateQueryChanRandomly(queryChan, indexSpec, m.NumQueries, m.NumRows)
	} else {
		benchChan := make(chan *Benchmark)
		cmdTypeChan := make(chan string)

		go readResults(m.QueryTemplate, benchChan, cmdTypeChan)
		if cmdType := <-cmdTypeChan; cmdType != cmdQuery {
			return errors.Errorf("given template is of type %s, not query", cmdType)
		}

		go populateQueryChanFromTemplate(benchChan, queryChan)
	}

	// run queries
	for i := 0; i < m.ThreadCount; i++ {
		go runQueries(clients, queryChan, qResultChans, m.ActualResults)
	}

	// process results
	processQueryResults(cmdQuery, qResultChans, path, m.ThreadCount)
	totalTime := time.Since(now)

	// process total time
	processTotalTime(totalTime, len(qResultChans), path, m.ThreadCount)

	return nil

}

// runQueries runs queries with nonnegative IDs from the query channel on all clusters,
// sending the results from the nth cluster to the nth channel in qResultChan.
func runQueries(clients []*pilosa.Client, queryChan chan Query, qResultChans []chan Query, actualResults bool) {
	if len(clients) != len(qResultChans) {
		panic("the number of clients does not match the number of channels for the results from these clients!")
	}

	for query := range queryChan {
		var wg sync.WaitGroup

		for i, client := range clients {
			wg.Add(1)
			go func(i int, client *pilosa.Client) {
				defer wg.Done()
				runQueryOnCluster(client, query, qResultChans[i], actualResults)
			}(i, client)
		}
		wg.Wait()
	}

	// there are no more queries left, so close the result channels
	for _, qResultChan := range qResultChans {
		close(qResultChan)
	}
}

// runQueryOnCluster runs a single query on a single cluster, sending the result to qResultChan.
func runQueryOnCluster(client *pilosa.Client, query Query, qResultChan chan Query, actualResults bool) {
	index, field, err := getIndexField(client, query.IndexName, query.FieldName)
	if err != nil {
		query.Error = errors.Errorf("error getting index %v and field %v for query with ID %v", query.IndexName, query.FieldName, query.ID)
		qResultChan <- query
		return
	}

	// build query
	rowQueries := make([]*pilosa.PQLRowQuery, 0, len(query.Rows))
	for _, rowNum := range query.Rows {
		rowQueries = append(rowQueries, field.Row(rowNum))
	}

	var rowQ pilosa.PQLQuery
	switch query.Type {
	case intersect:
		rowQ = index.Intersect(rowQueries...)
	case union:
		rowQ = index.Union(rowQueries...)
	case xor:
		rowQ = index.Xor(rowQueries...)
	case difference:
		rowQ = index.Difference(rowQueries...)
	default:
		query.Error = errors.Errorf("invalid query type: %v", query.Type)
		qResultChan <- query
		return
	}

	if !actualResults {
		rowQ = index.Count(rowQ.(*pilosa.PQLRowQuery))
	}

	now := time.Now()
	response, err := client.Query(rowQ)
	if err != nil {
		query.Error = errors.Wrapf(err, "could not query: %v", rowQ)
		qResultChan <- query
		return
	}

	query.Time.Duration = time.Since(now)

	if actualResults {
		result := response.Result().Row()
		query.Result = &result
	} else {
		resultCount := response.Result().Count()
		query.ResultCount = &resultCount
	}
	qResultChan <- query
}

// populateQueryChanRandomly populates the query channel with numQueries number of queries according to the specified specs
// and then closes the query channel.
func populateQueryChanRandomly(queryChan chan Query, indexSpec IndexSpec, numQueries int64, numRows int64) {
	for i := int64(0); i < numQueries; i++ {
		indexName, fieldName, err := indexSpec.randomIndexField()
		if err != nil {
			log.Fatalf("error getting random index and field from index spec: %+v", err)
		}
		min := indexSpec[indexName][fieldName].min
		max := indexSpec[indexName][fieldName].max
		rows, err := generateRandomRows(min, max, numRows)
		if err != nil {
			log.Fatalf("error generating random rows: %+v", err)
		}
		queryT := randomQueryType()

		query := Query{
			ID:        i,
			Type:      queryT,
			IndexName: indexName,
			FieldName: fieldName,
			Rows:      rows,
		}

		queryChan <- query
	}
	// numQueries queries have been sent, so channel can be closed
	close(queryChan)
}

// populateQueryChanFromTemplate populates the query channel with queries from the given template.
func populateQueryChanFromTemplate(benchChan chan *Benchmark, queryChan chan Query) {
	for bench := range benchChan {
		q := bench.Query
		query := Query {
			ID:        q.ID,
			Type:      q.Type,
			IndexName: q.IndexName,
			FieldName: q.FieldName,
			Rows:      q.Rows,
		}
		queryChan <- query
	}
	// all queries have been sent, so channel can be closed
	close(queryChan)
}

// processQueryResults writes the results of the nth channel to a file named n.
func processQueryResults(cmdType string, qResultChans []chan Query, dir string, threadcount int) {
	var wg sync.WaitGroup
	for i, qResultChan := range qResultChans {
		wg.Add(1)

		go func(i int, qResultChan chan Query) {
			defer wg.Done()
			filename := strconv.Itoa(i)
			writeQueryResults(cmdType, qResultChan, filename, dir, threadcount)
		}(i, qResultChan)
	}
	wg.Wait()
}

// processTotalTime appends the total time at the end of each file in path.
func processTotalTime(totalTime time.Duration, n int, path string, threadcount int) {
	// initialize channels
	totalTimeChans := make([]chan Query, 0)
	for i := 0; i < n; i++ {
		totalChan := make(chan Query)
		totalTimeChans = append(totalTimeChans, totalChan)
	}

	// initialize final result
	query := Query{
		ID:   -1,
		Time: TimeDuration{Duration: totalTime},
	}
	var wg sync.WaitGroup

	// send final result to channels
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			totalTimeChans[i] <- query
			close(totalTimeChans[i])
		}(i)
	}

	// append final results
	processQueryResults(cmdTotal, totalTimeChans, path, threadcount)
}

// writeQueryResults writes the query results from the channel to the file. If the file already exists,
// then the query results are appended to the file. cmdType can only be "query" or "total", which indicates
// that the query being written is the total benchmark.
func writeQueryResults(cmdType string, qResultChan chan Query, filename, dir string, threadcount int) {
	path := filepath.Join(dir, filename)
	fileWriter, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalf("could not create file %v: %+v", path, err)
	}

	encoder := json.NewEncoder(fileWriter)

	for q := range qResultChan {
		bench := &Benchmark{
			Type:        cmdType,
			Time:        q.Time,
			ThreadCount: threadcount,
			Query:       &q,
		}

		if err = encoder.Encode(bench); err != nil {
			log.Printf("error encoding %+v to %v: %v", q, filename, err)
		}
	}
}

// generateRandomRows generates numRows number of rows in the range of [min, max].
func generateRandomRows(min, max, numRows int64) ([]int64, error) {
	if min > max {
		return nil, errors.Errorf("min %v must be less than max %v", min, max)
	}
	rows := make([]int64, 0, numRows)
	for i := int64(0); i < numRows; i++ {
		// make sure row nums are in range
		rowNum := min + rand.Int63n(max-min+1)
		rows = append(rows, rowNum)
	}
	return rows, nil
}

type queryType byte

const (
	intersect queryType = iota
	union
	xor
	difference
	queryTypeCeiling // not an actual query
)

func (q queryType) String() string {
	switch q {
	case intersect:
		return "intersect"
	case union:
		return "union"
	case xor:
		return "xor"
	case difference:
		return "difference"
	default:
		return "invalid"
	}
}

// randomQueryType returns a random query type.
func randomQueryType() queryType {
	return queryType(rand.Intn(int(queryTypeCeiling)))
}

// IndexSpec maps indexes to the fields they contain.
type IndexSpec map[string]FieldSpec

// newIndexSpec initializes an empty IndexSpec struct.
func newIndexSpec() IndexSpec {
	return IndexSpec(make(map[string]FieldSpec))
}

// randomIndexField returns a random index and field from an indexSpec.
func (indexSpec IndexSpec) randomIndexField() (string, string, error) {
	if len(indexSpec) == 0 {
		return "", "", errors.New("index spec has no values")
	}

	// random index
	i := rand.Intn(len(indexSpec))
	var indexName string
	for indexName = range indexSpec {
		if i == 0 {
			break
		}
		i--
	}

	fieldSpec := indexSpec[indexName]
	if len(fieldSpec) == 0 {
		return "", "", errors.Errorf("index %v has zero fields", indexName)
	}
	// random field
	i = rand.Intn(len(fieldSpec))
	var fieldName string
	for fieldName = range fieldSpec {
		if i == 0 {
			break
		}
		i--
	}

	return indexName, fieldName, nil
}

// defaultIndexSpec creates an indexSpec mapping all indexes in the cluster to their fields.
func defaultIndexSpec(client *pilosa.Client) (IndexSpec, error) {
	schema, err := client.Schema()
	if err != nil {
		return nil, errors.Wrap(err, "error getting client schema")
	}

	indexSpec := newIndexSpec()

	for indexName, index := range schema.Indexes() {
		fieldSpec := newFieldSpec()
		for fieldName, field := range index.Fields() {
			min, max, err := getMinMaxRow(client, field)
			if err != nil {
				return nil, errors.Wrap(err, "error getting min and max row of field")
			}

			fieldSpec[fieldName] = pair{
				min: min,
				max: max,
			}
		}

		indexSpec[indexName] = fieldSpec
	}
	return indexSpec, nil
}

// defaultIndexSpecFromIndexes creates an indexSpec mapping the given indexes to their fields in the cluster.
func defaultIndexSpecFromIndexes(client *pilosa.Client, indexes []string) (IndexSpec, error) {
	schema, err := client.Schema()
	if err != nil {
		return nil, errors.Wrap(err, "error getting client schema")
	}

	indexSpec := newIndexSpec()

	// no indexes specified
	if indexes == nil || reflect.DeepEqual(indexes, []string{}) {
		indexSpec, err = defaultIndexSpec(client)
		if err != nil {
			return nil, errors.Wrap(err, "error getting default index spec")
		}
		return indexSpec, nil
	}

	// populate indexSpec with the given indexes
	for _, indexName := range indexes {
		if !schema.HasIndex(indexName) {
			return nil, errors.Errorf("schema does not have index %s", indexName)
		}
		index := schema.Index(indexName)

		fieldSpec := newFieldSpec()
		for fieldName, field := range index.Fields() {
			min, max, err := getMinMaxRow(client, field)
			if err != nil {
				return nil, errors.Wrap(err, "error getting min and max row of field")
			}

			fieldSpec[fieldName] = pair{
				min: min,
				max: max,
			}
		}

		indexSpec[indexName] = fieldSpec
	}
	return indexSpec, nil
}

// FieldSpec maps fields to their min and max rows.
type FieldSpec map[string]pair

// newFieldSpec initializes an empty FieldSpec struct.
func newFieldSpec() FieldSpec {
	return FieldSpec(make(map[string]pair))
}

// pair encodes the min and max of a field.
type pair struct{ min, max int64 }

// getIndexField returns the Pilosa index and field with the given names from the client.
func getIndexField(client *pilosa.Client, indexName, fieldName string) (*pilosa.Index, *pilosa.Field, error) {
	schema, err := client.Schema()
	if err != nil {
		return nil, nil, errors.Wrap(err, "error getting client schema")
	}

	if !schema.HasIndex(indexName) {
		return nil, nil, errors.Errorf("schema does not have index %s", indexName)
	}
	index := schema.Index(indexName)

	if !index.HasField(fieldName) {
		return nil, nil, errors.Errorf("index %s does not have field %s", indexName, fieldName)
	}
	field := index.Field(fieldName)

	return index, field, nil
}

// getMinMaxRow gets the min row and max row of a field.
func getMinMaxRow(client *pilosa.Client, field *pilosa.Field) (int64, int64, error) {
	response, err := client.Query(field.MinRow())
	if err != nil {
		return 0, 0, errors.Wrap(err, "error getting min row of field")
	}
	min := response.Result().CountItem().ID
	response, err = client.Query(field.MaxRow())
	if err != nil {
		return 0, 0, errors.Wrap(err, "error getting max row of field")
	}
	max := response.Result().CountItem().ID

	return int64(min), int64(max), nil
}
