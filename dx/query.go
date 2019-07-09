package dx

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/pilosa/go-pilosa"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

// NewQueryCommand initializes a new ingest command for dx.
func NewQueryCommand(m *Main) *cobra.Command {
	queryCmd := &cobra.Command{
		Use:   "query",
		Short: "perform random queries",
		Long:  `Perform randomly generated queries against both instances of Pilosa.`,
		Run: func(cmd *cobra.Command, args []string) {

			if err := ExecuteQueries(m); err != nil {
				fmt.Printf("%+v", err)
				os.Exit(1)
			}

		},
	}

	queryCmd.PersistentFlags().IntSliceVarP(&m.NumQueries, "queries", "q", []int{100}, "Number of queries to run")
	queryCmd.PersistentFlags().Int64VarP(&m.NumRows, "rows", "r", 2, "Number of rows to perform a query on")
	queryCmd.PersistentFlags().StringSliceVarP(&m.Indexes, "indexes", "i", nil, "Indexes to run queries on")
	queryCmd.PersistentFlags().BoolVarP(&m.ActualResults, "actualresults", "a", false, "Compare actual results of queries instead of counts")

	return queryCmd
}

const (
	queryIntersect byte = iota
	queryUnion
	queryXor
	queryDifference
	queryTypeMax // not actually an executable query
)

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

type indexConfig struct {
	name   string
	index  *pilosa.Index
	fields map[string]*fieldConfig
}

func newIndexConfig(name string) *indexConfig {
	return &indexConfig{
		name:   name,
		fields: make(map[string]*fieldConfig),
	}
}

type fieldConfig struct {
	name     string
	field    *pilosa.Field
	min, max int64
}

// holder contains the client and all indexes and fields
// that we are interested in within a cluster.
// this is unrelated to *pilosa.Holder.
type holder struct {
	client *pilosa.Client
	iconfs map[string]*indexConfig
}

func newHolder(iconfs map[string]*indexConfig) *holder {
	return &holder{
		iconfs: iconfs,
	}
}

func defaultHolder(hosts []string, port int) (*holder, error) {
	// initialize client
	client, err := initializeClient(hosts, port)
	if err != nil {
		return nil, errors.Wrap(err, "could not create client")
	}

	// initalize schema
	schema, err := client.Schema()
	if err != nil {
		return nil, errors.Wrap(err, "could not get client schema")
	}

	iconfs := make(map[string]*indexConfig)

	// get indexes and fields
	for indexName, index := range schema.Indexes() {
		indexConfig := newIndexConfig(indexName)
		indexConfig.index = index

		for fieldName, field := range index.Fields() {
			indexConfig.fields[fieldName] = &fieldConfig{
				name:  fieldName,
				field: field,
				// min:   int64(field.MinRow()),
				// max:   int64(field.MaxRow()),
			}
		}

		iconfs[indexName] = indexConfig
	}
	return &holder{
		client: client,
		iconfs: iconfs,
	}, nil
}

func defaultHolderWithIndexes(hosts []string, port int, indexNames []string) (*holder, error) {
	// initialize client
	client, err := initializeClient(hosts, port)
	if err != nil {
		return nil, errors.Wrap(err, "could not create client")
	}

	// initalize schema
	schema, err := client.Schema()
	if err != nil {
		return nil, errors.Wrap(err, "could not get client schema")
	}

	iconfs := make(map[string]*indexConfig)

	// get indexes and fields
	for _, indexName := range indexNames {
		if !schema.HasIndex(indexName) {
			return nil, errors.Errorf("schema does not have index %s", indexName)
		}
		index := schema.Index(indexName)
		indexConfig := newIndexConfig(indexName)
		indexConfig.index = index

		for fieldName, field := range index.Fields() {
			indexConfig.fields[fieldName] = &fieldConfig{
				name:  fieldName,
				field: field,
				// min:   int64(field.MinRow()),
				// max:   int64(field.MaxRow()),
			}
		}

		iconfs[indexName] = indexConfig
	}

	return &holder{
		client: client,
		iconfs: iconfs,
	}, nil
}

// CIF contains the pilosa client, indexes, and fields
// that we are interested in for a particular query.
type CIF struct {
	Client   *pilosa.Client
	Index    *pilosa.Index
	Field    *pilosa.Field
	Min, Max int64
}

// randomIF returns a random index and field from a holder.
func (holder *holder) randomIF() (string, string, error) {
	if len(holder.iconfs) == 0 {
		return "", "", errors.New("index config has zero elements")
	}

	// random index
	i := rand.Intn(len(holder.iconfs))
	var iconf *indexConfig
	for _, iconf = range holder.iconfs {
		if i == 0 {
			break
		}
		i--
	}

	if len(iconf.fields) == 0 {
		return "", "", errors.Errorf("index %v has zero fields", iconf.name)
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
		return nil, errors.Errorf("could not create CIF because index %v was not found", indexName)
	}
	iconf := holder.iconfs[indexName]
	if iconf.index == nil {
		return nil, errors.Errorf("could not create CIF because index %v is nil", iconf.name)
	}

	if _, found := iconf.fields[fieldName]; !found {
		return nil, errors.Errorf("could not create CIF because field %v was not found in index %v", fieldName, indexName)
	}
	fconf := iconf.fields[fieldName]
	if fconf.field == nil {
		return nil, errors.Errorf("could not create CIF because field %v is nil", fconf.name)
	}

	return &CIF{
		Client: holder.client,
		Index:  iconf.index,
		Field:  fconf.field,
		Min:    fconf.min,
		Max:    fconf.max,
	}, nil
}

// ExecuteQueries executes the random queries on both Pilosa instances repeatedly
// according to the values specified in m.NumQueries.
func ExecuteQueries(m *Main) error {
	var cHolder, pHolder *holder
	var err error

	if m.Indexes == nil {
		cHolder, err = defaultHolder(m.CHosts, m.CPort)
		if err != nil {
			return errors.Wrap(err, "could not create holder for candidate")
		}
		pHolder, err = defaultHolder(m.PHosts, m.PPort)
		if err != nil {
			return errors.Wrap(err, "could not create holder for primary")
		}
	} else {
		cHolder, err = defaultHolderWithIndexes(m.CHosts, m.CPort, m.Indexes)
		if err != nil {
			return errors.Wrap(err, "could not create holder for candidate with the indexes")
		}
		pHolder, err = defaultHolderWithIndexes(m.PHosts, m.PPort, m.Indexes)
		if err != nil {
			return errors.Wrap(err, "could not create holder for primary with the indexes")
		}
	}

	// run benchmarks
	qBench := make([]*Benchmark, 0, len(m.NumQueries))
	for _, numQueries := range m.NumQueries {
		q, err := executeQueries(cHolder, pHolder, numQueries, m.ThreadCount, m.NumRows, m.ActualResults)
		if err != nil {
			return errors.Wrap(err, "could not execute query")
		}
		qBench = append(qBench, q)
	}

	// print results
	if err := printQueryResults(qBench...); err != nil {
		return errors.Wrap(err, "could not print")
	}
	return nil
}

// queryOp is a hacky solution to allow launchThreads to be reused in query and solo query
// TODO: refactor
type queryOp struct {
	holder     *holder
	cHolder    *holder
	pHolder    *holder
	resultChan chan *QResult
	queryChan  chan *Query
	numRows    int64
	actualRes  bool
}

func executeQueries(cHolder, pHolder *holder, numQueries, threadCount int, numRows int64, actualRes bool) (*Benchmark, error) {
	qResultChan := make(chan *QResult, numQueries)
	// TODO: check for pass by reference
	q := &queryOp{
		cHolder:    cHolder,
		pHolder:    pHolder,
		resultChan: qResultChan,
		numRows:    numRows,
		actualRes:  actualRes,
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
		if q.resultChan != nil {
			close(q.resultChan)
		}
		if q.queryChan != nil {
			close(q.queryChan)
		}
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
		log.Printf("could not get random index-field from candidate holder: %v", err)
		qResult.err = errors.Wrap(err, "could not get random index-field from candidate holder")
		qResultChan <- qResult
		return
	}
	cCIF, err := cHolder.newCIF(indexName, fieldName)
	if err != nil {
		log.Printf("could not find index %v and field %v from candidate holder: %v", indexName, fieldName, err)
		qResult.err = errors.Wrapf(err, "could not find index %v and field %v from candidate holder", indexName, fieldName)
		qResultChan <- qResult
		return
	}
	pCIF, err := pHolder.newCIF(indexName, fieldName)
	if err != nil {
		log.Printf("could not find index %v and field %v from primary holder: %v", indexName, fieldName, err)
		qResult.err = errors.Wrapf(err, "could not find index %v and field %v from primary holder", indexName, fieldName)
		qResultChan <- qResult
		return
	}

	// generate random row numbers to query in both instances
	rows, err := generateRandomRows(cCIF.Min, cCIF.Max, numRows)
	if err != nil {
		log.Printf("could not generate reandom rows: %v", err)
		return
	}
	queryType := randomQueryType()

	go runQueryOnInstance(cCIF, queryType, rows, cResultChan, q.actualRes)
	go runQueryOnInstance(pCIF, queryType, rows, pResultChan, q.actualRes)

	cResult := <-cResultChan
	pResult := <-pResultChan

	qResult.candidateTime = cResult.time
	qResult.primaryTime = pResult.time
	if cResult.err != nil || pResult.err != nil {
		// log detailed error
		log.Printf("error querying: candidate error: %+v, primary error: %+v\n", cResult.err, pResult.err)
		qResult.err = fmt.Errorf("error querying: candidate error: %v, primary error: %v", cResult.err, pResult.err)
		qResultChan <- qResult
		return
	}
	if q.actualRes {
		if reflect.DeepEqual(cResult.result, pResult.result) {
			qResult.correct = true
		} else {
			log.Printf("different results:\ncandidate:\n%v\nprimary\n%v\n", cResult.result, pResult.result)
		}
	} else {
		if cResult.resultCount == pResult.resultCount {
			qResult.correct = true
		} else {
			log.Printf("different result counts: candidate: %v, primary %v", cResult.resultCount, pResult.resultCount)
		}
	}

	qResultChan <- qResult
}

func generateRandomRows(min, max, numRows int64) ([]int64, error) {
	if min > max {
		return nil, errors.Errorf("min %v must be less than max %v", min, max)
	}
	rows := make([]int64, 0, numRows)
	for i := int64(0); i < numRows; i++ {
		// make sure row nums are in range
		rowNum := min
		if min < max {
			rowNum += rand.Int63n(max - min)
		}
		rows = append(rows, rowNum)
	}
	return rows, nil
}

func randomQueryType() byte {
	return byte(rand.Intn(int(queryTypeMax)))
}

func runQueryOnInstance(cif *CIF, queryType byte, rows []int64, resultChan chan *Result, actualRes bool) {
	result := NewResult()

	// build query
	rowQueries := make([]*pilosa.PQLRowQuery, 0, len(rows))
	for _, rowNum := range rows {
		rowQueries = append(rowQueries, cif.Field.Row(rowNum))
	}

	var rowQ pilosa.PQLQuery
	switch queryType {
	case queryIntersect:
		rowQ = cif.Index.Intersect(rowQueries...)
	case queryUnion:
		rowQ = cif.Index.Union(rowQueries...)
	case queryXor:
		rowQ = cif.Index.Xor(rowQueries...)
	case queryDifference:
		rowQ = cif.Index.Difference(rowQueries...)
	default:
		result.err = errors.Errorf("invalid query type: %v", queryType)
		resultChan <- result
		return
	}

	if !actualRes {
		rowQ = cif.Index.Count(rowQ.(*pilosa.PQLRowQuery))
	}

	now := time.Now()
	response, err := cif.Client.Query(rowQ)
	if err != nil {
		result.err = errors.Wrapf(err, "could not query: %v", rowQ)
		resultChan <- result
		return
	}

	result.time = time.Since(now)

	if actualRes {
		result.result = response.Result()
	} else {
		result.resultCount = response.Result().Count()
	}
	resultChan <- result
}
