package bench

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/pilosa/go-pilosa"
)

// Benchmark is an interface run benchmark components. Benchmarks should Marshal
// to valid JSON so that their configuration may be recorded with their results.
type Benchmark interface {
	Run(ctx context.Context, client *pilosa.Client, agentNum int) (*Result, error)
}

type HostSetup struct {
	Hosts         []string
	ClientOptions []pilosa.ClientOption
}

// Results holds the output from the run of a benchmark - the Benchmark's Run()
// method may set Stats, Responses, and Extra, and the RunBenchmark helper
// function will set the Duration, AgentNum, PilosaVersion, and Configuration.
// Either may set Error if there is an error. The structure of Result assumes
// that most benchmarks will run multiple queries and track statistics about how
// long each one takes. The Extra field is for benchmarks which either do not
// fit this model, or want to return additional information not covered by Stats
// and Responses.
type Result struct {
	Stats         *Stats                  `json:"stats"`
	Responses     []*pilosa.QueryResponse `json:"responses"`
	Extra         map[string]interface{}  `json:"extra"`
	AgentNum      int                     `json:"agentnum"`
	PilosaVersion string                  `json:"pilosa-version"`
	Configuration interface{}             `json:"configuration"`

	// Error exists so that errors can be correctly marshalled to JSON. It is set using Result.err.Error()
	Error string `json:"error,omitempty"`
}

// NewResult intializes and returns a Result.
func NewResult() *Result {
	return &Result{
		Stats:     NewStats(),
		Extra:     make(map[string]interface{}),
		Responses: make([]*pilosa.QueryResponse, 0),
	}
}

// Add adds the duration to the Result's Stats object. If resp is non-nil, it
// also adds it to the slice of responses.
func (r *Result) Add(d time.Duration, resp *pilosa.QueryResponse) {
	r.Stats.Add(d)
	if resp != nil {
		r.Responses = append(r.Responses, resp)
	}
}

// ensureSchema ensures that a given index and field exist.
func ensureSchema(client *pilosa.Client, indexName, fieldName string, opts ...interface{}) (index *pilosa.Index, field *pilosa.Field, err error) {
	var indexOpts []pilosa.IndexOption
	var fieldOpts []pilosa.FieldOption
	for _, opt := range opts {
		switch opt := opt.(type) {
		case pilosa.IndexOption:
			indexOpts = append(indexOpts, opt)
		case pilosa.FieldOption:
			fieldOpts = append(fieldOpts, opt)
		}
	}

	schema, err := client.Schema()
	if err != nil {
		return nil, nil, fmt.Errorf("cannot read schema: %v", err)
	}

	index = schema.Index(indexName, indexOpts...)
	if err := client.EnsureIndex(index); err != nil {
		return nil, nil, fmt.Errorf("cannot ensure index: %v", err)
	}
	if fieldName != "" {
		field = index.Field(fieldName, fieldOpts...)
		if err := client.EnsureField(field); err != nil {
			return nil, nil, fmt.Errorf("cannot ensure field: %v", err)
		}
	}
	if err := client.SyncSchema(schema); err != nil {
		return nil, nil, fmt.Errorf("cannot sync schema: %v", err)
	}
	return index, field, nil
}

// PermutationGenerator provides a way to pass integer IDs through a permutation
// map that is pseudorandom but repeatable. This could be done with rand.Perm,
// but that would require storing a [Iterations]int64 array, which we want to avoid
// for large values of Iterations.
// It works by using a Linear Congruence Generator (https://en.wikipedia.org/wiki/Linear_congruential_generator)
// with modulus m = Iterations,
// c = an arbitrary prime,
// a = computed to ensure the full period.
// relevant stackoverflow: http://cs.stackexchange.com/questions/29822/lazily-computing-a-random-permutation-of-the-positive-integers
type PermutationGenerator struct {
	a int64
	c int64
	m int64
}

func NewPermutationGenerator(m int64, seed int64) *PermutationGenerator {
	// figure out 'a' and 'c', return PermutationGenerator
	a := LCGmultiplierFromModulus(m, seed)
	c := int64(22695479)
	return &PermutationGenerator{a, c, m}
}

func (p *PermutationGenerator) Next(n int64) int64 {
	// run one step of the LCG
	return (n*p.a + p.c) % p.m
}

// LCG parameters must satisfy three conditions:
// 1. m and c are relatively prime (satisfied for prime c != m)
// 2. a-1 is divisible by all prime factors of m
// 3. a-1 is divisible by 4 if m is divisible by 4
// Additionally, a seed can be used to select between different permutations
func LCGmultiplierFromModulus(m int64, seed int64) int64 {
	factors := primeFactors(m)
	product := int64(1)
	for p := range factors {
		// satisfy condition 2
		product *= p
	}

	if m%4 == 0 {
		// satisfy condition 3
		product *= 2
	}

	return product*seed + 1
}

// Returns map of {integerFactor: count, ...}
// This is a naive algorithm that will not work well for large prime n.
func primeFactors(n int64) map[int64]int {
	factors := make(map[int64]int)
	for i := int64(2); i <= n; i++ {
		div, mod := n/i, n%i
		for mod == 0 {
			factors[i] += 1
			n = div
			div, mod = n/i, n%i
		}
	}
	return factors
}

// wrapper type to force human-readable JSON output
type PrettyDuration time.Duration

// MarshalJSON returns a nicely formatted duration, instead of it just being
// treated like an int.
func (d PrettyDuration) MarshalJSON() ([]byte, error) {
	s := time.Duration(d).String()
	return []byte("\"" + s + "\""), nil
}

// Recursively replaces elements of ugly types with their pretty wrappers
func Prettify(m map[string]interface{}) map[string]interface{} {
	newmap := make(map[string]interface{})
	for k, v := range m {
		switch v.(type) {
		case map[string]interface{}:
			newmap[k] = Prettify(v.(map[string]interface{}))
		case []time.Duration:
			newslice := make([]PrettyDuration, len(v.([]time.Duration)))
			slice := v.([]time.Duration)
			for n, e := range slice {
				newslice[n] = PrettyDuration(e)
			}
			newmap[k] = newslice
		case time.Duration:
			newmap[k] = PrettyDuration(v.(time.Duration))
		default:
			if interv, ok := v.([]map[string]interface{}); ok {
				for i, iv := range interv {
					interv[i] = Prettify(iv)
				}
			}
			newmap[k] = v
		}
	}
	return newmap
}

// NewQueryGenerator returns a new QueryGenerator.
func NewQueryGenerator(index *pilosa.Index, field *pilosa.Field, seed int64) *QueryGenerator {
	return &QueryGenerator{
		index: index,
		field: field,
		rand:  rand.New(rand.NewSource(seed)),
	}
}

// QueryGenerator holds the configuration and state for randomly generating queries.
type QueryGenerator struct {
	index *pilosa.Index
	field *pilosa.Field
	rand  *rand.Rand
}

// Random returns a randomly generated query.
func (g *QueryGenerator) Random(maxN, depth, maxargs int, idmin, idmax uint64) pilosa.PQLQuery {
	val := g.rand.Intn(5)
	switch val {
	case 0:
		return g.RandomTopN(maxN, depth, maxargs, idmin, idmax)
	default:
		return g.RandomBitmapCall(depth, maxargs, idmin, idmax)
	}
}

// RandomRangeQuery returns a randomly generated sum or range query.
func (g *QueryGenerator) RandomRangeQuery(depth, maxargs int, idmin, idmax uint64) pilosa.PQLQuery {
	switch g.rand.Intn(5) {
	case 1:
		return g.RandomSum(depth, maxargs, idmin, idmax)
	default:
		return g.RandomRange(maxargs, idmin, idmax)
	}
}

func (g *QueryGenerator) RandomRange(numArg int, idmin, idmax uint64) *pilosa.PQLRowQuery {
	choose := g.rand.Intn(4)
	if choose == 0 {
		return g.RangeCall(idmin, idmax)
	}
	a := make([]*pilosa.PQLRowQuery, numArg)
	for i := 0; i < numArg; i++ {
		a[i] = g.RangeCall(idmin, idmax)
	}

	switch choose {
	case 1:
		return g.index.Difference(a...)
	case 2:
		return g.index.Intersect(a...)
	case 3:
		return g.index.Union(a...)
	default:
		panic("unreachable")
	}
}

func (g *QueryGenerator) RangeCall(idmin, idmax uint64) *pilosa.PQLRowQuery {
	const operationN = 5
	switch g.rand.Intn(operationN) {
	case 0:
		return g.field.GT(g.rand.Intn(int(idmax - idmin)))
	case 1:
		return g.field.LT(g.rand.Intn(int(idmax - idmin)))
	case 2:
		return g.field.GTE(g.rand.Intn(int(idmax - idmin)))
	case 3:
		return g.field.LTE(g.rand.Intn(int(idmax - idmin)))
	case 4:
		return g.field.Equals(g.rand.Intn(int(idmax - idmin)))
	default:
		panic("unreachable")
	}
}

// RandomSum returns a randomly generated sum query.
func (g *QueryGenerator) RandomSum(depth, maxargs int, idmin, idmax uint64) pilosa.PQLQuery {
	switch g.rand.Intn(4) {
	case 0:
		return g.field.Sum(g.RandomBitmapCall(depth, maxargs, idmin, idmax))
	default:
		return g.field.Sum(g.RandomRange(maxargs, idmin, idmax))
	}
}

// RandomTopN returns a randomly generated TopN query.
func (g *QueryGenerator) RandomTopN(maxN, depth, maxargs int, idmin, idmax uint64) *pilosa.PQLRowQuery {
	return g.field.RowTopN(uint64(g.rand.Intn(maxN-1)+1), g.RandomBitmapCall(depth, maxargs, idmin, idmax))
}

// RandomBitmapCall returns a randomly generate query which returns a bitmap.
func (g *QueryGenerator) RandomBitmapCall(depth, maxargs int, idmin, idmax uint64) *pilosa.PQLRowQuery {
	if depth <= 1 {
		return g.field.Row(uint64(g.rand.Int63n(int64(idmax)-int64(idmin)) + int64(idmin)))
	}
	choose := g.rand.Intn(4)
	if choose == 0 {
		return g.RandomBitmapCall(1, 0, idmin, idmax)
	}

	numargs := 2
	if maxargs > 2 {
		numargs = g.rand.Intn(maxargs-2) + 2
	}
	a := make([]*pilosa.PQLRowQuery, numargs)
	for i := 0; i < numargs; i++ {
		a[i] = g.RandomBitmapCall(depth-1, maxargs, idmin, idmax)
	}

	switch choose {
	case 1:
		return g.index.Difference(a...)
	case 2:
		return g.index.Intersect(a...)
	case 3:
		return g.index.Union(a...)
	default:
		panic("unreachable")
	}
}

// Stats object helps track timing stats.
type Stats struct {
	sumSquareDelta float64

	Min     time.Duration   `json:"min"`
	Max     time.Duration   `json:"max"`
	Mean    time.Duration   `json:"mean"`
	Total   time.Duration   `json:"total-time"`
	Num     int64           `json:"num"`
	All     []time.Duration `json:"all"`
	SaveAll bool            `json:"-"`
}

// NewStats gets a Stats object.
func NewStats() *Stats {
	return &Stats{
		Min: 1<<63 - 1,
		All: make([]time.Duration, 0),
	}
}

// Add adds a new time to the stats object.
func (s *Stats) Add(td time.Duration) {
	if s.SaveAll {
		s.All = append(s.All, td)
	}
	s.Num += 1
	s.Total += td
	if td < s.Min {
		s.Min = td
	}
	if td > s.Max {
		s.Max = td
	}

	// online variance calculation
	// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm
	delta := td - s.Mean
	s.Mean += delta / time.Duration(s.Num)
	s.sumSquareDelta += float64(delta * (td - s.Mean))
}
