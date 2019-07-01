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

// QResult is the result of querying two instances of Pilosa.
// TODO: Replace this with Benchmark
type QResult struct {
	correct       bool // if both results are the same
	candidateTime time.Duration
	primaryTime   time.Duration
	err           error
}

// newQResult returns a new QResult.
func newQResult() *QResult {
	return &QResult{
		correct:       false,
		candidateTime: 0,
		primaryTime:   0,
	}
}

// holder contains the client and all indexes and fields
// that we are interested in within a cluster.
// this is unrelated to *pilosa.Holder.
type holder struct {
	instance string
	client *pilosa.Client
	iconfs map[string]*indexConfig
}

func initializeHolder(instanceType string, hosts []string, port int, iconfs map[string]*indexConfig) (*holder, error) {
	// make sure newIconfs and iconfs do not point to same array
	newIconfs := deepcopy(iconfs)

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

	// initialize indexes
	for _, iconf := range newIconfs {
		if !schema.HasIndex(iconf.name) {
			return nil, fmt.Errorf("could not find index %v", iconf.name)
		}
		// set pilosa index
		iconf.index = schema.Index(iconf.name)

		// initalize fields
		for _, fconf := range iconf.fields {
			if !iconf.index.HasField(fconf.name) {
				return nil, fmt.Errorf("could not find field %v in index %v", fconf.name, iconf.name)
			}
			// set pilosa field
			fconf.field = iconf.index.Field(fconf.name)
		}
	}
	return &holder{
		client: client,
		iconfs: newIconfs,
	}, nil
}

// CIF contains the pilosa client, indexes, and fields
// that we are interested in for a particular query.
type CIF struct {
	Client   *pilosa.Client
	Index    *pilosa.Index
	Field    *pilosa.Field
	Columns  uint64
	Min, Max int64
}

//TODO: check for errors
// randomIF returns a random index and field from a holder.
func (holder *holder) randomIF() (string, string, error) {
	// random index
	i := rand.Intn(len(holder.iconfs))
	var iconf *indexConfig
	for _, iconf = range holder.iconfs {
		if i == 0 {
			break
		}
		i--
	}

	// random field
	i = rand.Intn(len(iconf.fields))
	var fconf *fieldConfig
	for _, fconf = range iconf.fields {
		if i == 0 {
			break
		}
		i--
	}

	return iconf.name, fconf.name, nil
}

func (holder *holder) newCIF(indexName, fieldName string) (*CIF, error) {
	if _, found := holder.iconfs[indexName]; !found {
		return nil, fmt.Errorf("could not create CIF because index %v was not found", indexName)
	}
	iconf := holder.iconfs[indexName]
	if iconf.index == nil {
		return nil, fmt.Errorf("could not create CIF because index %v is nil", iconf.name)
	}
	
	if _, found := iconf.fields[fieldName]; !found {
		return nil, fmt.Errorf("could not create CIF because field %v was not found in index %v", fieldName, indexName)
	}
	fconf := iconf.fields[fieldName]
	if fconf.field == nil {
		return nil, fmt.Errorf("could not create CIF because field %v is nil", fconf.name)
	}

	return &CIF{
		Client:  holder.client,
		Index:   iconf.index,
		Field:   fconf.field,
		Columns: iconf.columns,
		Min:     fconf.min,
		Max:     fconf.max,
	}, nil
}

// ExecuteQueries executes the random queries on both Pilosa instances repeatedly
// according to the values specified in m.NumQueries.
func ExecuteQueries() error {
	iconfs, err := getSpecs(m.SpecsFile)
	if err != nil {
		return fmt.Errorf("could not parse specs: %v", err)
	}

	// initialize holders
	cHolder, err := initializeHolder(instanceCandidate, m.CHosts, m.CPort, iconfs)
	if err != nil {
		return fmt.Errorf("could not create holder for candidate: %v", err)
	}
	pHolder, err := initializeHolder(instancePrimary, m.PHosts, m.PPort, iconfs)
	if err != nil {
		return fmt.Errorf("could not create holder for primary: %v", err)
	}
	
	// run benchmarks
	qBench := make([]*Benchmark, 0, len(m.NumQueries))
	for _, numQueries := range m.NumQueries {
		q, err := executeQueries(cHolder, pHolder, numQueries, m.ThreadCount, m.NumRows)
		if err != nil {
			return fmt.Errorf("could not execute query: %v", err)
		}
		qBench = append(qBench, q)
	}

	// print results
	if err := printQueryResults(qBench...); err != nil {
		return fmt.Errorf("could not print: %v", err)
	}
	return nil
}

// queryOp is a hacky solution to allow launchThreads to be reused in query and solo query
// TODO: refactor
type queryOp struct {
	holder		*holder
	cHolder		*holder
	pHolder		*holder
	resultChan	chan *QResult
	queryChan	chan *Query
	numRows		int64
}

func executeQueries(cHolder, pHolder *holder, numQueries, threadCount int, numRows int64) (*Benchmark, error) {
	qResultChan := make(chan *QResult, numQueries)
	// TODO: check for pass by reference
	q := &queryOp {
		cHolder:	cHolder,
		pHolder:	pHolder,
		resultChan:	qResultChan,
		numRows:	numRows,
	}
	go launchThreads(numQueries, threadCount, q, runQuery)

	return analyzeQueryResults(qResultChan, numQueries)
}

func analyzeQueryResults(resultChan chan *QResult, numQueries int) (*Benchmark, error) {
	var cTotal time.Duration
	var pTotal time.Duration
	var numCorrect int64

	// validQueries is the number of queries that successfully ran.
	// This is not equivalent to the number of queries with correct results.
	validQueries := numQueries

	for qResult := range resultChan {
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
	return &Benchmark{
		Size:      int64(validQueries),
		Accuracy:  accuracy,
		CTime:     cTotal,
		PTime:     pTotal,
		TimeDelta: timeDelta,
	}, nil
}

// this is where we allocate threadCount number of goroutines
// to execute the numQueries number of queries. workQueue does
// not contain meaningful values and only exists to distribute
// the workload among the channels.
func launchThreads(numQueries, threadCount int, q *queryOp, fn func(*queryOp)) {
	workQueue := make(chan bool, numQueries)
	var wg sync.WaitGroup

	for i := 0; i < threadCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range workQueue {
				fn(q)
			}
		}()
	}
	for i := 0; i < numQueries; i++ {
		workQueue <- true
	}
	close(workQueue)
	go func() {
		wg.Wait()
		close(q.resultChan)
		close(q.queryChan)
	}()
}

// runQuery runs a query on both Pilosa instances.
func runQuery(q *queryOp) {
	cHolder, pHolder := q.cHolder, q.pHolder
	qResultChan := q.resultChan
	numRows := q.numRows
	
	qResult := newQResult()

	cResultChan := make(chan *Result, 1)
	pResultChan := make(chan *Result, 1)

	// initialize CIFs
	indexName, fieldName, err := cHolder.randomIF()
	if err != nil {
		m.Logger.Printf("could not get random index-field from candidate holder: %v", err)
		qResult.err = fmt.Errorf("could not get random index-field from candidate holder: %v", err)
		qResultChan <- qResult
		return
	}
	cCIF, err := cHolder.newCIF(indexName, fieldName)
	if err != nil {
		m.Logger.Printf("could not find index %v and field %v from candidate holder: %v", indexName, fieldName, err)
		qResult.err = fmt.Errorf("could not find index %v and field %v from candidate holder: %v", indexName, fieldName, err)
		qResultChan <- qResult
		return
	}
	pCIF, err := pHolder.newCIF(indexName, fieldName)
	if err != nil {
		m.Logger.Printf("could not find index %v and field %v from primary holder: %v", indexName, fieldName, err)
		qResult.err = fmt.Errorf("could not find index %v and field %v from primary holder: %v", indexName, fieldName, err)
		qResultChan <- qResult
		return
	}

	// TODO: verify cCIF == pCIF
	// generate random row numbers to query in both instances
	rows := generateRandomRows(cCIF.Min, cCIF.Max, numRows)

	go runQueryOnInstance(cCIF, rows, cResultChan)
	go runQueryOnInstance(pCIF, rows, pResultChan)

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

func generateRandomRows(min, max, numRows int64) ([]int64) {
	rows := make([]int64, 0, numRows)
	for i := int64(0); i < numRows; i++ {
		// make sure row nums are in range
		rowNum := min + rand.Int63n(max-min)
		rows = append(rows, rowNum)
	}
	return rows
}

func runQueryOnInstance(cif *CIF, rows []int64, resultChan chan *Result) {
	result := NewResult()

	// build query
	rowQueries := make([]*pilosa.PQLRowQuery, 0, len(rows))
	for _, rowNum := range rows {
		rowQueries = append(rowQueries, cif.Field.Row(rowNum))
	}
	rowQ := cif.Index.Intersect(rowQueries...)

	// run query
	now := time.Now()
	response, err := cif.Client.Query(rowQ)
	if err != nil {
		result.err = fmt.Errorf("could not query: %v", err)
		resultChan <- result
		return
	}

	result.time = time.Since(now)
	result.result = response.Result()

	resultChan <- result
}
