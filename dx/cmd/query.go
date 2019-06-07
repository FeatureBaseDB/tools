package dx

import (
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/pilosa/go-pilosa"
	"github.com/spf13/cobra"
)

// NewQueryCommand initializes a new ingest command for dx.
func NewQueryCommand() *cobra.Command {
	ingestCmd := &cobra.Command{
		Use:   "query",
		Short: "perform random queries",
		Long:  `Perform randomly generated queries against both instances of Pilosa.`,
		Run: func(cmd *cobra.Command, args []string) {

			if err := ExecuteQueries(); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

		},
	}
	ingestCmd.PersistentFlags().IntSliceVarP(&m.NumQueries, "queries", "q", []int{100000, 1000000, 10000000, 100000000}, "Number of queries to run")
	ingestCmd.PersistentFlags().Int64VarP(&m.NumRows, "rows", "r", 2, "Number of rows to perform intersect query on")
	return ingestCmd
}

// QueryBenchmark is the result of executing a query benchmark.
// This is the result passed on to printing.
type QueryBenchmark struct {
	Queries   int
	Accuracy  float64
	CTime     time.Duration // candidate total time
	PTime     time.Duration // primary total time
	TimeDelta float64
}

// QResult is the result of querying two instances of Pilosa.
type QResult struct {
	correct       bool // if both results are the same
	candidateTime time.Duration
	primaryTime   time.Duration
	err           error
}

// NewQResult returns a new QResult.
func NewQResult() *QResult {
	return &QResult{
		correct:       false,
		candidateTime: 0,
		primaryTime:   0,
	}
}

// CSIF is a shorthand struct for denoting the specific
// client, schema, index, and field we are interested in.
type CSIF struct {
	Client *pilosa.Client
	Schema *pilosa.Schema
	Index  *pilosa.Index
	Field  *pilosa.Field
}

func newCSIF(hosts []string, port int, indexName, fieldName string) (*CSIF, error) {
	// initialize client
	client, err := initializeClient(hosts, port)
	if err != nil {
		return nil, fmt.Errorf("could not create client: %v", err)
	}

	// initalize schema
	schema, err := client.Schema()
	if err != nil {
		return nil, fmt.Errorf("could not get client schema: %v", err)
	}
	if !schema.HasIndex(indexName) {
		return nil, fmt.Errorf("could not find index %v", indexName)
	}

	// initalize index and field
	index := schema.Index(indexName)
	if !index.HasField(fieldName) {
		return nil, fmt.Errorf("could not find field %v in index %v", fieldName, indexName)
	}
	field := index.Field(fieldName)

	return &CSIF{
		Client: client,
		Schema: schema,
		Index:  index,
		Field:  field,
	}, nil
}

// ExecuteQueries executes the random queries on both Pilosa instances repeatedly
// according to the values specified in m.NumQueries.
func ExecuteQueries() error {
	qBench := make([]*QueryBenchmark, 0, len(m.NumQueries))
	for _, numQueries := range m.NumQueries {
		q, err := executeQueries(numQueries)
		if err != nil {
			return fmt.Errorf("could not execute query: %v", err)
		}
		qBench = append(qBench, q)
	}

	if err := printQueryResults(qBench...); err != nil {
		return fmt.Errorf("could not print: %v", err)
	}
	return nil
}

func executeQueries(numQueries int) (*QueryBenchmark, error) {
	specs, err := getSpecs(m.SpecsFile)
	if err != nil {
		fmt.Println("using default specs values")
	}

	// initialize CSIF for candidate and primary
	cCSIF, err := newCSIF(m.CHosts, m.CPort, specs.indexName, specs.fieldName)
	if err != nil {
		return nil, fmt.Errorf("could not create csif for candidate: %v", err)
	}
	pCSIF, err := newCSIF(m.PHosts, m.PPort, specs.indexName, specs.fieldName)
	if err != nil {
		return nil, fmt.Errorf("could not create csif for primary: %v", err)
	}

	// this is where we allocate ThreadCount number of goroutines
	// to execute the numQueries number of queries. workQueue does
	// not contain meaningful values and only exists to distribute
	// the workload among the channels.
	qResultChan := make(chan *QResult, numQueries)
	workQueue := make(chan bool, numQueries)
	var wg sync.WaitGroup

	for i := 0; i < m.ThreadCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range workQueue {
				runQuery(cCSIF, pCSIF, specs.min, specs.max, qResultChan)
			}
		}()
	}
	for i := 0; i < numQueries; i++ {
		workQueue <- true
	}
	close(workQueue)
	go func() {
		wg.Wait()
		close(qResultChan)
	}()

	// analysis and printing
	var cTotal time.Duration
	var pTotal time.Duration
	var numCorrect int64

	// validQueries is the number of queries that successfully ran.
	// This is not equivalent to the number of correct queries.
	validQueries := numQueries

	for qResult := range qResultChan {
		if qResult.err != nil {
			validQueries--
		} else {
			cTotal += qResult.candidateTime
			pTotal += qResult.primaryTime
			if qResult.correct {
				numCorrect++
			}
		}
	}

	accuracy := float64(numCorrect) / float64(validQueries)
	timeDelta := float64(cTotal-pTotal) / float64(pTotal)
	return &QueryBenchmark{
		Queries:   validQueries,
		Accuracy:  accuracy,
		CTime:     cTotal,
		PTime:     pTotal,
		TimeDelta: timeDelta,
	}, nil
}

// runQuery runs a query on both Pilosa instances.
func runQuery(cCSIF, pCSIF *CSIF, min, max int64, qResultChan chan *QResult) {
	qResult := NewQResult()

	cResultChan := make(chan *Result, 1)
	pResultChan := make(chan *Result, 1)

	// generate random row numbers to query in both instances
	rows := make([]int64, 0, m.NumRows)
	for i := int64(0); i < m.NumRows; i++ {
		rowNum := min + rand.Int63n(max-min)
		rows = append(rows, rowNum)
	}

	go runQueryOnInstance(cCSIF, rows, cResultChan)
	go runQueryOnInstance(pCSIF, rows, pResultChan)

	cResult := <-cResultChan
	pResult := <-pResultChan

	qResult.candidateTime = cResult.time
	qResult.primaryTime = pResult.time
	if cResult.err != nil || pResult.err != nil {
		m.Logger.Printf("error querying: candidate error: %v, primary error: %v\n", cResult.err, pResult.err)
		qResult.err = fmt.Errorf("error querying: candidate error: %v, primary error: %v", cResult.err, pResult.err)
		qResultChan <- qResult
		return
	}

	if reflect.DeepEqual(cResult.result, pResult.result) {
		qResult.correct = true
	} else {
		m.Logger.Printf("different results:\ncandidate:\n%v\nprimary\n%v\n", cResult.result, pResult.result)
	}

	qResultChan <- qResult
}

func runQueryOnInstance(csif *CSIF, rows []int64, resultChan chan *Result) {
	result := NewResult()

	// build query
	rowQueries := make([]*pilosa.PQLRowQuery, 0, len(rows))
	for _, rowNum := range rows {
		rowQueries = append(rowQueries, csif.Field.Row(rowNum))
	}
	rowQ := csif.Index.Intersect(rowQueries...)

	// run query
	now := time.Now()
	response, err := csif.Client.Query(rowQ)
	if err != nil {
		result.err = fmt.Errorf("could not query: %v", err)
		resultChan <- result
		return
	}

	result.time = time.Since(now)
	result.result = response.Result()

	resultChan <- result
}
