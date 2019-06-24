package imagine

//go:generate enumer -type=verifyType -trimprefix=verifyType -text -transform=kebab -output enums_verifytype.go

import (
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"sync"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/jaffee/commandeer"
	pilosa "github.com/pilosa/go-pilosa"
	"github.com/pkg/errors"
	flag "github.com/spf13/pflag"
)

var (
	client *pilosa.Client
)

type verifyType int

const (
	verifyTypeError verifyType = iota
	verifyTypeNone
	verifyTypePurge
	verifyTypeUpdate
	verifyTypeCreate
)

// Config describes the overall configuration of the tool.
type Config struct {
	Hosts        []string `help:"comma separated host names for Pilosa servers"`
	Port         int      `help:"host port for Pilosa server"`
	Verify       string   `help:"index structure validation: purge/error/update/create"`
	verifyType   verifyType
	Generate     bool `help:"generate data as specified by workloads"`
	Delete       bool `help:"delete specified indexes"`
	Describe     bool `help:"describe the data sets and workloads"`
	onlyDescribe bool
	Prefix       string `help:"prefix to use on index names"`
	CPUProfile   string `help:"record CPU profile to file"`
	MemProfile   string `help:"record allocation profile to file"`
	Time         bool   `help:"report on time elapsed for operations"`
	Status       bool   `help:"show status updates while processing"`
	ColumnScale  int64  `help:"scale number of columns provided by specs"`
	RowScale     int64  `help:"scale number of rows provided by specs"`
	LogImports   string `help:"file name to log all imports to (so they can be replayed later)"`
	ThreadCount  int    `help:"number of threads to use for each import, overrides value set in config file"`
	flagset      *flag.FlagSet
	specFiles    []string
	specs        []*tomlSpec
	indexes      map[string]*indexSpec
	workloads    []namedWorkload
	dbSchema     map[string]map[string]*pilosa.Field
}

// Run does validation on the configuration data. Used by
// commandeer.
func (conf *Config) Run() error {
	// no error-checking if nothing to check errors on
	if conf == nil {
		return nil
	}
	if int(uint16(conf.Port)) != conf.Port {
		return fmt.Errorf("port %d out of range", conf.Port)
	}
	if conf.ThreadCount < 0 {
		return fmt.Errorf("invalid thread count %d [must be a positive number]", conf.ThreadCount)
	}
	// if not given other instructions, just describe the specs
	if !conf.Generate && !conf.Delete && conf.Verify == "" {
		conf.Describe = true
		conf.onlyDescribe = true
	}
	if conf.Verify != "" {
		err := conf.verifyType.UnmarshalText([]byte(conf.Verify))
		if err != nil {
			return fmt.Errorf("unknown verify type '%s'", conf.Verify)
		}
	} else {
		// If we're deleting, and not generating, we can skip
		// verification. Otherwise, default to erroring out if
		// there's a mismatch.
		if conf.Delete && !conf.Generate {
			conf.verifyType = verifyTypeNone
		} else {
			conf.verifyType = verifyTypeError
		}
	}
	conf.NewSpecsFiles(conf.flagset.Args())
	if len(conf.specFiles) < 1 {
		return errors.New("must specify one or more spec files")
	}
	if conf.ColumnScale < 0 || conf.ColumnScale > (1<<31) {
		return fmt.Errorf("column scale [%d] should be between 1 and 2^31", conf.ColumnScale)
	}
	if conf.RowScale < 0 || conf.RowScale > (1<<16) {
		return fmt.Errorf("row scale [%d] should be between 1 and 2^16", conf.RowScale)
	}
	return nil
}

// ReadSpecs reads the files in conf.specFiles and populates fields.
func (conf *Config) ReadSpecs() error {
	conf.specs = make([]*tomlSpec, 0, len(conf.specFiles))
	conf.indexes = make(map[string]*indexSpec, len(conf.specFiles))
	for _, path := range conf.specFiles {
		spec, err := readSpec(path)
		if err != nil {
			return fmt.Errorf("couldn't read spec '%s': %v", path, err)
		}
		// here is where we put overrides like setting the prefix
		// from command-line parameters before doing more validation and
		// populating inferred fields.
		if spec.Prefix == "" {
			spec.Prefix = conf.Prefix
		}
		err = spec.CleanupIndexes(conf)
		if err != nil {
			return err
		}
		conf.specs = append(conf.specs, spec)
	}
	for _, spec := range conf.specs {
		err := spec.CleanupWorkloads(conf)
		if err != nil {
			return err
		}
	}
	return nil
}

// NewConfig initializes a config struct with default/initial values,
// which can be overridden by command line options.
func NewConfig() *Config {
	return &Config{
		Hosts:       []string{"localhost"},
		Port:        10101,
		Generate:    true,
		Verify:      "update",
		Prefix:      "imaginary-",
		ThreadCount: 0, // if unchanged, uses workloadspec.threadcount
		// if workloadspec.threadcount is also unset, defaults to 1
	}
}

// Execute executes the imagine command.
func (conf *Config) Execute() {
	go func() {
		fmt.Printf("failed to start pprof server on 6060: %v\n", http.ListenAndServe("localhost:6060", nil))
	}()

	conf.flagset = flag.NewFlagSet("", flag.ContinueOnError)

	err := commandeer.RunArgs(conf.flagset, conf, os.Args[1:])
	if err != nil {
		log.Fatalf("parsing arguments: %s", err)
	}

	err = conf.ReadSpecs()
	if err != nil {
		log.Fatalf("config/spec error: %v", err)
	}

	// dry run: just describe the indexes and stop there.
	if conf.Describe {
		for _, spec := range conf.specs {
			describeSpec(spec)
		}
		// if we weren't asked to do anything else, stop here.
		if conf.onlyDescribe {
			os.Exit(0)
		}
	}

	uris := make([]*pilosa.URI, 0, len(conf.Hosts))
	for _, host := range conf.Hosts {
		uri, err := pilosa.NewURIFromHostPort(host, uint16(conf.Port))
		if err != nil {
			log.Fatalf("could not create Pilosa URI: %v", err)
		}
		uris = append(uris, uri)
	}

	clientOpts := make([]pilosa.ClientOption, 0)
	if conf.LogImports != "" {
		f, err := os.Create(conf.LogImports)
		if err != nil {
			log.Fatalf("Couldn't open conf.LogImports: '%s'. Err: %v", conf.LogImports, err)
		}
		clientOpts = append(clientOpts, pilosa.ExperimentalOptClientLogImports(f))
	}

	cluster := pilosa.NewClusterWithHost(uris...)
	client, err := pilosa.NewClient(cluster, clientOpts...)
	if err != nil {
		log.Fatalf("could not create Pilosa client: %v", err)
	}

	serverInfo, err := client.Info()
	if err != nil {
		log.Fatalf("couldn't get server info: %v", err)
	}
	serverMemMB := serverInfo.Memory / (1024 * 1024)
	// this is probably really stupid.
	serverMemGB := (serverMemMB + 1023) / 1024
	fmt.Printf("server memory: %dGB [%dMB]\n", serverMemGB, serverMemMB)
	fmt.Printf("server CPU: %s [%d physical cores, %d logical cores available]\n", serverInfo.CPUType, serverInfo.CPUPhysicalCores, serverInfo.CPULogicalCores)
	serverStatus, err := client.Status()
	if err != nil {
		log.Fatalf("couldn't get cluster status info: %v", err)
	}
	fmt.Printf("cluster nodes: %d\n", len(serverStatus.Nodes))

	// start profiling only after all the startup decisions are made
	if conf.CPUProfile != "" {
		f, cErr := os.Create(conf.CPUProfile)
		if cErr != nil {
			log.Fatalf("can't create CPU profile '%s': %s", conf.CPUProfile, cErr)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if conf.MemProfile != "" {
		defer func() {
			f, mErr := os.Create(conf.MemProfile)
			if mErr != nil {
				fmt.Fprintf(os.Stderr, "can't create memory profile '%s': %s", conf.MemProfile, mErr)
			} else {
				pprof.Lookup("allocs").WriteTo(f, 0)
			}
		}()
	}

	// do verification.
	switch conf.verifyType {
	case verifyTypeNone:
		// do nothing
	case verifyTypeError:
		err = conf.VerifyIndexes(client)
	case verifyTypePurge:
		err = conf.DeleteIndexes(client)
		if err == nil {
			err = conf.CreateIndexes(client)
		}
	case verifyTypeUpdate:
		err = conf.UpdateIndexes(client)
	case verifyTypeCreate:
		err = conf.CreateIndexes(client)
	}
	if err != nil {
		log.Fatalf("initial validation: %v", err)
	}

	if conf.Generate {
		err := conf.ApplyWorkloads(client)
		if err != nil {
			log.Fatalf("applying workloads: %v", err)
		}
	}

	if conf.Delete {
		err := conf.DeleteIndexes(client)
		if err != nil {
			log.Fatalf("deleting indexes: %v", err)
		}
	}

	fmt.Printf("done.\n")
}

// NewSpecsFiles copies files to config.specFiles.
func (conf *Config) NewSpecsFiles(files []string) {
	conf.specFiles = files
}

// CompareIndexes is the general form of comparing client and server index
// specs. mayCreate indicates that it's acceptable to create an index if it's
// missing. mustCreate indicates that it's mandatory to create an index, so
// it must not have previously existed.
func (conf *Config) CompareIndexes(client *pilosa.Client, mayCreate, mustCreate bool) error {
	errs := make([]error, 0)
	schema, err := client.Schema()
	if err != nil {
		return errors.Wrap(err, "retrieving schema")
	}
	dbIndexes := schema.Indexes()
	var dbIndex *pilosa.Index
	changed := false
	for _, index := range conf.indexes {
		dbIndex = dbIndexes[index.FullName]
		if dbIndex == nil {
			if !mayCreate {
				errs = append(errs, fmt.Errorf("index '%s' does not exist", index.FullName))
				continue
			}
			changed = true
			dbIndex = schema.Index(index.FullName)
		} else {
			if mustCreate {
				errs = append(errs, fmt.Errorf("index '%s' already exists", index.FullName))
				continue
			}
		}
		// if we got here, the index now exists, so we can do the same thing to fields...
		changedFields, fieldErrs := conf.CompareFields(client, dbIndex, index, mayCreate, mustCreate)
		if changedFields {
			changed = true
		}
		errs = append(errs, fieldErrs...)
	}
	if changed {
		fmt.Printf("changes made to db, syncing...\n")
		err = client.SyncSchema(schema)
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// CompareFields checks the individual fields in an index, following the same
// logic as CompareIndexes. mayCreate indicates that it's acceptable to create
// an index if it's missing. mustCreate indicates that it's mandatory to create
// an index, so it must not have previously existed.
func (conf *Config) CompareFields(client *pilosa.Client, dbIndex *pilosa.Index, spec *indexSpec, mayCreate, mustCreate bool) (changed bool, errs []error) {
	dbFields := dbIndex.Fields()
	for name, field := range spec.FieldsByName {
		dbField := dbFields[name]
		if dbField == nil {
			if !mayCreate {
				errs = append(errs, fmt.Errorf("field '%s' does not exist in '%s'", name, spec.FullName))
				continue
			}
		} else {
			if mustCreate {
				errs = append(errs, fmt.Errorf("field '%s' already exists in '%s'", name, spec.FullName))
				continue
			}
			// field exists. we don't have a way to check it yet, so we continue.
			continue
		}
		changed = true
		switch field.Type {
		case fieldTypeInt:
			dbIndex.Field(name, pilosa.OptFieldTypeInt(int64(field.Min), int64(field.Max)))
		case fieldTypeSet:
			dbIndex.Field(name, pilosa.OptFieldTypeSet(pilosa.CacheType(field.Cache.String()), field.CacheSize))
		case fieldTypeMutex:
			dbIndex.Field(name, pilosa.OptFieldTypeMutex(pilosa.CacheType(field.Cache.String()), field.CacheSize))
		case fieldTypeTime:
			dbIndex.Field(name, pilosa.OptFieldTypeTime(pilosa.TimeQuantum(field.Quantum.String())))
		default:
			errs = append(errs, fmt.Errorf("unknown field type '%s'", field.Type))
		}
	}
	return changed, errs
}

// VerifyIndexes verifies that the indexes and fields specified already
// exist and match.
func (conf *Config) VerifyIndexes(client *pilosa.Client) error {
	return conf.CompareIndexes(client, false, false)
}

// CreateIndexes attempts to create indexes, erroring out if any exist.
func (conf *Config) CreateIndexes(client *pilosa.Client) error {
	return conf.CompareIndexes(client, true, true)
}

// UpdateIndexes attempts to create indexes or fields, accepting existing
// things as long as they match.
func (conf *Config) UpdateIndexes(client *pilosa.Client) error {
	return conf.CompareIndexes(client, true, false)
}

// DeleteIndexes attempts to delete all specified indexes.
func (conf *Config) DeleteIndexes(client *pilosa.Client) error {
	schema, err := client.Schema()
	if err != nil {
		return errors.Wrap(err, "retrieving schema")
	}
	dbIndexes := schema.Indexes()
	for _, index := range conf.indexes {
		if dbIndexes[index.FullName] != nil {
			err = client.DeleteIndex(dbIndexes[index.FullName])
			if err != nil {
				return errors.Wrapf(err, "deleting index '%s'", index.FullName)
			}
		}
	}
	return nil
}

// ApplyNamedWorkload attempts to process each workload in a named workload.
func (conf *Config) ApplyNamedWorkload(client *pilosa.Client, nwl namedWorkload) (err error) {
	if conf.Time {
		before := time.Now()
		fmt.Printf("beginning workloads from spec %s\n", nwl.SpecName)
		defer func() {
			after := time.Now()
			var completed = "completed"
			if err != nil {
				completed = "failed"
			}
			fmt.Printf("spec %s %s in %v\n", nwl.SpecName, completed, after.Sub(before))
		}()
	}
	// now apply each workload
	for _, wl := range nwl.Workloads {
		err = conf.ApplyWorkload(client, wl)
		if err != nil {
			return err
		}
	}
	return nil
}

// ApplyWorkload attempts to process a workload.
func (conf *Config) ApplyWorkload(client *pilosa.Client, wl *workloadSpec) (err error) {
	if conf.Time {
		before := time.Now()
		fmt.Printf(" beginning workload %s\n", wl.Name)
		defer func() {
			after := time.Now()
			var completed = "completed"
			if err != nil {
				completed = "failed"
			}
			fmt.Printf(" workload %s %s in %v\n", wl.Name, completed, after.Sub(before))
		}()
	}
	for _, batch := range wl.Batches {
		err = conf.ApplyBatch(client, batch)
		if err != nil {
			return err
		}
	}
	return nil
}

type taskUpdate struct {
	id       string
	colCount int64
	rowCount int64
	done     bool
}

// ApplyBatch attempts to process the configured batch.
func (conf *Config) ApplyBatch(client *pilosa.Client, batch *batchSpec) (err error) {
	var tasks sync.WaitGroup
	if conf.Time {
		before := time.Now()
		fmt.Printf("  beginning batch %s\n", batch.Description)
		defer func() {
			after := time.Now()
			var completed = "completed"
			if err != nil {
				completed = "failed"
			}
			fmt.Printf("  workload %s %s in %v\n", batch.Description, completed, after.Sub(before))
		}()
	}
	// and now, in parallel...
	errs := make([]error, len(batch.Tasks))
	updateChan := make(chan taskUpdate, len(batch.Tasks))
	generatorUpdateChan := updateChan
	if !conf.Status {
		generatorUpdateChan = nil
	}
	for idx, task := range batch.Tasks {
		field := conf.dbSchema[task.IndexFullName][task.Field]
		if field == nil {
			errs[idx] = fmt.Errorf("index '%s', field '%s' not found in schema", task.IndexFullName, task.Field)
			continue
		}
		itr, opts, err := NewGenerator(task, generatorUpdateChan, task.Index+"/"+task.Field)
		if err != nil {
			errs[idx] = err
			continue
		}
		tasks.Add(1)
		go func(idx int, itr CountingIterator, opts []pilosa.ImportOption, field *pilosa.Field, indexName, fieldName string) {
			before := time.Now()
			errs[idx] = client.ImportField(field, itr, opts...)
			if conf.Time {
				after := time.Now()
				v, t := itr.Values()
				fmt.Printf("   %s/%s: %v for %d/%d values\n", indexName, fieldName, after.Sub(before), v, t)
			}
			tasks.Done()
		}(idx, itr, opts, field, task.Index, task.Field)
	}
	go func() {
		tasks.Wait()
		close(updateChan)
	}()
	for u := range updateChan {
		if u.rowCount != 0 {
			fmt.Printf("    %s %10d/%-10d\r", u.id, u.colCount, u.rowCount)
		} else {
			fmt.Printf("    %s %-10d\r", u.id, u.colCount)
		}
	}
	if !conf.Time {
		// without the trailing time update, the later "done" overwrites
		// part of the status line.
		fmt.Println("")
	}
	errorCount := 0
	err = nil
	for _, e := range errs {
		if e != nil {
			errorCount++
			if err == nil {
				err = e
			}
		}
	}
	if errorCount < 2 {
		return err
	}
	return errors.Wrap(err, fmt.Sprintf("%d errors", errorCount))
}

// ApplyWorkloads attempts to process the configured workloads.
func (conf *Config) ApplyWorkloads(client *pilosa.Client) error {
	// grab all the schema data for easy lookup
	schema, err := client.Schema()
	if err != nil {
		return err
	}
	indexes := schema.Indexes()
	conf.dbSchema = make(map[string]map[string]*pilosa.Field, len(indexes))
	for name, index := range indexes {
		conf.dbSchema[name] = index.Fields()
	}
	for _, nwl := range conf.workloads {
		err = conf.ApplyNamedWorkload(client, nwl)
		if err != nil {
			return err
		}
	}
	return nil
}
