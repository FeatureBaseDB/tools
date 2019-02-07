package main

import (
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/jaffee/commandeer"
	pilosa "github.com/pilosa/go-pilosa"
	"github.com/pkg/errors"
	flag "github.com/spf13/pflag"
)

var (
	client *pilosa.Client
)

// Config describes the overall configuration of the tool.
type Config struct {
	Host          string `help:"host name for Pilosa server"`
	Port          int    `help:"host port for Pilosa server"`
	Create        bool   `help:"create indexes if needed"`
	Delete        bool   `help:"delete existing indexes with same prefix"`
	DryRun        bool   `help:"dry-run; describe what would be done"`
	Prefix        string `help:"prefix to use on index names"`
	defaultPrefix bool
	Spec          string `help:"filename of .toml spec file"`
	CPUProfile    string `help:"record CPU profile to file"`
	MemProfile    string `help:"record allocation profile to file"`
	Time          bool   `help:"report on time elapsed for a spec"`
	Overwrite     bool   `help:"allow writing into existing indexes"`
}

// Run does validation on the configuration data. Used by
// commandeer.
func (c *Config) Run() error {
	// no error-checking if nothing to check errors on
	if c == nil {
		return nil
	}
	if int(uint16(c.Port)) != c.Port {
		return fmt.Errorf("port %d out of range", c.Port)
	}
	if c.Prefix == "" {
		c.defaultPrefix = true
		c.Prefix = "imaginary"
	}
	if c.Spec == "" {
		return errors.New("must specify a spec file (--spec)")
	}

	// If no action is specified, create indexes but don't delete them.
	if !c.Create && !c.Delete && !c.DryRun {
		c.Create = true
	}
	return nil
}

func (c *Config) readSpec() (*tomlSpec, error) {
	if c.Spec == "" {
		return nil, errors.New("you must have a spec file [--spec=]")
	}
	spec, err := readSpec(c.Spec)
	if err != nil {
		return nil, fmt.Errorf("couldn't read spec: %s", err)
	}
	// here is where we put overrides like setting the prefix
	// from command-line parameters before doing more validation and
	// populating inferred fields.
	if c.defaultPrefix == false || spec.Prefix == "" {
		spec.Prefix = c.Prefix
	}
	err = spec.Cleanup()
	if err != nil {
		return nil, err
	}
	return spec, nil
}

func main() {
	// Conf defines the default/initial values for config, which
	// can be overridden by command line options.
	conf := &Config{
		Host: "localhost",
		Port: 10101,
		Spec: "",
	}
	flagset := flag.NewFlagSet("", flag.ContinueOnError)

	err := commandeer.RunArgs(flagset, conf, os.Args[1:])
	if err != nil {
		log.Fatalf("parsing arguments: %s", err)
	}

	spec, err := conf.readSpec()
	if err != nil {
		log.Fatalf("config/spec error: %s", err)
	}

	// dry run: just describe the indexes and stop there.
	if conf.DryRun {
		describeIndexes(spec)
		os.Exit(0)
	}

	uri, err := pilosa.NewURIFromHostPort(conf.Host, uint16(conf.Port))
	if err != nil {
		log.Fatalf("could not create Pilosa URI: %s", err)
	}

	client, err = pilosa.NewClient(uri)
	if err != nil {
		log.Fatalf("could not create Pilosa client: %s", err)
	}

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

	if conf.Delete {
		err = deleteIndexes(client, spec)
		if err != nil {
			log.Fatalf("deleting indexes: %s", err.Error())
		}
	}

	if conf.Create {
		err = createIndexes(client, spec, conf)
		if err != nil {
			log.Fatalf("creating indexes: %s", err.Error())
		}
	}
}

func deleteIndexes(client *pilosa.Client, spec *tomlSpec) error {
	schema, err := client.Schema()
	if err != nil {
		return errors.Wrap(err, "retrieving schema")
	}
	dbIndexes := schema.Indexes()
	for _, index := range spec.Indexes {
		if dbIndexes[index.FullName] != nil {
			err = client.DeleteIndex(dbIndexes[index.FullName])
			if err != nil {
				return errors.Wrapf(err, "deleting index '%s'", index.FullName)
			}
		}
	}
	return nil
}

func createIndexes(client *pilosa.Client, spec *tomlSpec, conf *Config) error {
	// yes, we might already have it from deleteIndexes, but it might also
	// be stale.
	schema, err := client.Schema()
	if err != nil {
		return errors.Wrap(err, "retrieving schema")
	}
	dbIndexes := schema.Indexes()
	for _, index := range spec.Indexes {
		if _, ok := dbIndexes[index.FullName]; ok && !conf.Overwrite {
			return fmt.Errorf("index '%s' already exists [%#v]", index.FullName, *dbIndexes[index.FullName])
		}
		newIndex := schema.Index(index.FullName)
		err = createIndex(client, spec, index, newIndex, conf)
		if err != nil {
			return err
		}
	}
	err = client.SyncSchema(schema)
	if err != nil {
		return err
	}
	schema, err = client.Schema()
	if err != nil {
		return err
	}
	dbIndexes = schema.Indexes()
	for _, index := range spec.Indexes {
		err = populateIndex(client, spec, index, dbIndexes[index.FullName], conf)
		if err != nil {
			return err
		}
	}
	return nil
}

func createIndex(client *pilosa.Client, spec *tomlSpec, iSpec *indexSpec, index *pilosa.Index, conf *Config) error {
	for name, field := range iSpec.Fields {
		switch field.Type {
		case fieldTypeBSI:
			if field.SourceIndex != "" {
				var other *indexSpec
				for _, otherIndex := range spec.Indexes {
					if otherIndex.Name == field.SourceIndex {
						other = otherIndex
					}
				}
				if other == nil {
					return fmt.Errorf("sourceIndex '%s' not defined", field.SourceIndex)
				}

				field.Min, field.Max = 0, int64(other.Columns)
				// fmt.Printf("SourceIndex %s: min/max %d/%d\n", field.SourceIndex, field.Min, field.Max)
				// and stash that back in the map
				iSpec.Fields[name] = field
			}
			min, max := field.Min, field.Max
			if max <= min {
				return fmt.Errorf("invalid range %d to %d", min, max)
			}
			index.Field(name, pilosa.OptFieldTypeInt(int64(min), int64(max)))
		case fieldTypeSet:
			index.Field(name, pilosa.OptFieldTypeSet("none", 0))
		case fieldTypeMutex:
			index.Field(name, pilosa.OptFieldTypeMutex("none", 0))
		default:
			return fmt.Errorf("unknown field type '%s'", field.Type)
		}
	}
	return nil
}

func populateIndex(client *pilosa.Client, spec *tomlSpec, iSpec *indexSpec, index *pilosa.Index, conf *Config) error {
	var imports sync.WaitGroup
	var errors []*error
	threadCount := iSpec.ThreadCount
	if threadCount < 1 {
		threadCount = 1
	}

	importField := func(field string, d CountingIterator) {
		imports.Add(1)
		var errPtr = new(error)
		errors = append(errors, errPtr)
		go func() {
			before := time.Now()
			*errPtr = client.ImportField(index.Field(field), d, pilosa.OptImportThreadCount(threadCount))
			if conf.Time {
				after := time.Now()
				fmt.Printf("%s/%s: %v for %d values\n", iSpec.Name, field, after.Sub(before), d.Values())
			}
			imports.Done()
		}()
	}
	before := time.Now()
	fmt.Printf("populating '%s' table, %d columns: %s\n", iSpec.Name, iSpec.Columns, iSpec.Description)
	for name, fs := range iSpec.Fields {
		iter, err := NewGenerator(fs)
		if err != nil {
			err = fmt.Errorf("%s: %s", name, err.Error())
			errors = append(errors, &err)
			continue
		}
		importField(name, iter)
	}
	imports.Wait()
	if conf.Time {
		after := time.Now()
		fmt.Printf("%s: %v\n", iSpec.Name, after.Sub(before))
	}
	fmt.Printf("done with '%s'.\n", iSpec.Name)
	for _, ep := range errors {
		if *ep != nil {
			return *ep
		}
	}
	return nil
}
