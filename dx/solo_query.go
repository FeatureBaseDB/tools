package dx

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/pilosa/go-pilosa"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

// NewSoloQueryCommand initializes a new query command for dx.
func NewSoloQueryCommand(m *Main) *cobra.Command {
	sQueryCmd := &cobra.Command{
		Use:   "query",
		Short: "perform random queries",
		Long:  `Perform randomly generated queries on a single instances of Pilosa.`,
		Run: func(cmd *cobra.Command, args []string) {

			if err := ExecuteSoloQueries(m); err != nil {
				fmt.Printf("%+v", err)
				os.Exit(1)
			}

		},
	}

	// TODO: add flag specifying indexes to run queries on only
	sQueryCmd.PersistentFlags().IntSliceVarP(&m.NumQueries, "queries", "q", []int{100}, "Number of queries to run")
	sQueryCmd.PersistentFlags().Int64VarP(&m.NumRows, "rows", "r", 2, "Number of rows to perform intersect query on")
	sQueryCmd.PersistentFlags().StringSliceVarP(&m.Indexes, "indexes", "i", nil, "Indexes to run queries on")
	sQueryCmd.PersistentFlags().StringVar(&m.QueryTemplate, "query-template", "query_template", "File name of query template to use")
	sQueryCmd.PersistentFlags().BoolVarP(&m.ActualResults, "actualresults", "a", true, "Compare actual results of queries instead of counts")

	return sQueryCmd
}

// SoloBenchmark represents the final result of a dx solo query benchmark.
type SoloBenchmark struct {
	Type          string            `json:"type"`
	Time          TimeDuration      `json:"time,omitempty"`
	NumBenchmarks *int              `json:"numBenchmarks,omitempty"`
	Benchmarks    []*QueryBenchmark `json:"benchmarks,omitempty"`
	ThreadCount   *int              `json:"threadcount,omitempty"`
	ActualResults *bool             `json:"actualresults,omitempty"`
}

func newSoloBenchmark(benchType string, threadCount int) *SoloBenchmark {
	return &SoloBenchmark{
		Type:        benchType,
		Benchmarks:  make([]*QueryBenchmark, 0),
		ThreadCount: &threadCount,
	}
}

// QueryBenchmark represents a single query benchmark.
type QueryBenchmark struct {
	NumQueries int            `json:"numQueries"`
	Time       TimeDuration   `json:"time"`
	Queries    map[int]*Query `json:"queries"`
}

func newQueryBenchmark(numQueries int) *QueryBenchmark {
	return &QueryBenchmark{
		NumQueries: numQueries,
		Queries:    make(map[int]*Query),
	}
}

// Query contains the information related to a single query.
type Query struct {
	ID          int               `json:"id"`
	IndexName   string            `json:"index"`
	FieldName   string            `json:"field"`
	Type        byte              `json:"query"`
	Rows        []int64           `json:"rows"`
	Result      *pilosa.RowResult `json:"result,omitempty"`
	ResultCount *int64            `json:"resultcount,omitempty"`
	Time        TimeDuration      `json:"time"`
	Error       error             `json:"-"`
}

// ExecuteSoloQueries executes queries on a single Pilosa instance. If QueryTemplate does not exist, it will generate that
// first before running the generated queries.
func ExecuteSoloQueries(m *Main) error {
	var holder *holder
	var err error

	if m.Indexes == nil {
		holder, err = defaultHolder(m.Hosts, m.Port)
		if err != nil {
			return errors.Wrap(err, "could not create holder")
		}
	} else {
		holder, err = defaultHolderWithIndexes(m.Hosts, m.Port, m.Indexes)
		if err != nil {
			return errors.Wrap(err, "could not create holder")
		}
	}

	isFirstQuery, err := checkBenchIsFirst(m.QueryTemplate, m.DataDir)
	if err != nil {
		return errors.Wrap(err, "error checking if prior bench exists")
	}

	if isFirstQuery {
		if err = generateQueriesTemplate(holder, m.QueryTemplate, m.DataDir, m.NumQueries, m.ThreadCount, m.NumRows, m.ActualResults); err != nil {
			return errors.Wrap(err, "could not generate random queries template")
		}
	}

	if m.Filename == "" {
		m.Filename = cmdQuery + "-" + time.Now().Format(time.RFC3339Nano)
	}

	if err = executeSoloQueries(holder, m.QueryTemplate, m.DataDir); err != nil {
		return errors.Wrap(err, "could not execute solo queries")
	}
	return nil
}

// generateQueriesTemplate generates a query template for dx solo query to run on future instances. The template is saved to dataDir with
// file name specified by the variable queryTemplate.
func generateQueriesTemplate(holder *holder, queryTemplate, dataDir string, numBenchmarks []int, threadCount int, numRows int64, actualRes bool) error {
	solobench := newSoloBenchmark(cmdQuery, threadCount)
	solobench.ActualResults = &actualRes
	for _, numQueries := range numBenchmarks {
		querybench := newQueryBenchmark(numQueries)

		// generate queries
		for i := 0; i < numQueries; i++ {
			indexName, fieldName, err := holder.randomIF()
			if err != nil {
				return errors.Wrap(err, "could not generate random index and field from holder")
			}
			cif, err := holder.newCIF(indexName, fieldName)
			if err != nil {
				return errors.Wrapf(err, "could not create index %v and field %v from holder", indexName, fieldName)
			}
			rows, err := generateRandomRows(cif.Min, cif.Max, numRows)
			if err != nil {
				return errors.Wrap(err, "could not generate random rows")
			}
			queryType := randomQueryType()

			query := &Query{
				ID:        i,
				IndexName: indexName,
				FieldName: fieldName,
				Type:      queryType,
				Rows:      rows,
			}
			querybench.Queries[i] = query
		}

		solobench.Benchmarks = append(solobench.Benchmarks, querybench)
	}

	// write the query template to file
	if err := writeQueryResultFile(solobench, queryTemplate, dataDir); err != nil {
		return errors.Wrap(err, "could not write queries template to file")
	}
	return nil
}

// executeSoloQueries reads the query template file and executes those queries.
func executeSoloQueries(holder *holder, filename, dataDir string) error {
	solobench, err := readResultFile(filename, dataDir)
	if err != nil {
		return errors.Wrap(err, "could not read query result file")
	}
	if solobench.Type != cmdQuery {
		return errors.Errorf("running dx solo query, but query template shows command: %v", solobench.Type)
	}

	for _, querybench := range solobench.Benchmarks {
		if err := runSoloBenchmark(holder, querybench, *solobench.ThreadCount, *solobench.ActualResults); err != nil {
			return errors.Wrap(err, "could not run queries from the queries template")
		}
	}

	// write modified solobench to file
	if err = writeQueryResultFile(solobench, filename, dataDir); err != nil {
		return errors.Wrap(err, "could not write result to file")
	}

	return nil
}

// runSoloBenchmark launches threadcount number of goroutines to run the queries, modifying querybench in place.
func runSoloBenchmark(holder *holder, querybench *QueryBenchmark, threadCount int, actualRes bool) error {
	numQueries := querybench.NumQueries
	queryChan := make(chan *Query, numQueries)

	for _, query := range querybench.Queries {
		queryChan <- query
	}
	q := &queryOp{
		holder:    holder,
		queryChan: queryChan,
		actualRes: actualRes,
	}
	go launchThreads(numQueries, threadCount, q, runQueriesOnInstance)

	for query := range q.queryChan {
		querybench.Time.Duration += query.Time.Duration
	}
	return nil
}

// runQueriesOnInstance runs a query received from the query channel, modifying that query in place.
func runQueriesOnInstance(q *queryOp) {
	resultChan := make(chan *Result, 1)

	query := <-q.queryChan
	cif, err := q.holder.newCIF(query.IndexName, query.FieldName)
	if err != nil {
		log.Printf("could not create index %v and field %v from holder", query.IndexName, query.FieldName)
		return
	}

	go runQueryOnInstance(cif, query.Type, query.Rows, resultChan, q.actualRes)
	result := <-resultChan
	if result.err != nil {
		log.Printf("error running query on instance: %v", err)
		return
	}

	// save the results of query in place
	if q.actualRes {
		rowResult := result.result.Row()
		query.Result = &rowResult
	} else {
		query.ResultCount = &result.resultCount
	}
	query.Time.Duration = result.time
}

// writeQueryResulFile writes the results of a SoloBenchmark to a JSON file.
func writeQueryResultFile(bench *SoloBenchmark, filename, dataDir string) error {
	if err := os.MkdirAll(dataDir, 0777); err != nil {
		return errors.Wrap(err, "error making data directory")
	}

	jsonBytes, err := json.Marshal(bench)
	if err != nil {
		return errors.Wrap(err, "could not marshal results to JSON")
	}

	path := filepath.Join(dataDir, filename)
	if err = ioutil.WriteFile(path, jsonBytes, 0666); err != nil {
		return errors.Wrap(err, "could not write JSON to file")
	}
	return nil
}

// readResultFile reads the results of a SoloBenchmark from a file.
func readResultFile(filename, dataDir string) (*SoloBenchmark, error) {
	path := filepath.Join(dataDir, filename)

	file, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrap(err, "could not open previous result file")
	}
	defer file.Close()

	jsonBytes, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, errors.Wrap(err, "could not read previous result file")
	}
	var soloBench SoloBenchmark
	json.Unmarshal(jsonBytes, &soloBench)

	return &soloBench, nil
}
