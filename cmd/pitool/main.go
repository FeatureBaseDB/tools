package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pilosa/tools/bench"
	"github.com/pilosa/tools/build"
	"github.com/pilosa/tools/creator"
	pssh "github.com/pilosa/tools/ssh"

	"github.com/satori/go.uuid"
	"golang.org/x/crypto/ssh"
)

var (
	// ErrUnknownCommand is returned when specifying an unknown command.
	ErrUnknownCommand = errors.New("unknown command")
)

func main() {
	m := NewMain()

	// Parse command line arguments.
	if err := m.ParseFlags(os.Args[1:]); err == flag.ErrHelp {
		os.Exit(2)
	} else if err != nil {
		fmt.Fprintln(m.Stderr, err)
		os.Exit(2)
	}

	// Execute the program.
	if err := m.Run(); err != nil {
		fmt.Fprintln(m.Stderr, err)
		os.Exit(1)
	}
}

// Main represents the main program execution.
type Main struct {
	// Subcommand to execute.
	Cmd Command

	// Standard input/output
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// NewMain returns a new instance of Main.
func NewMain() *Main {
	return &Main{
		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

// Usage returns the usage message to be printed.
func (m *Main) Usage() string {
	return strings.TrimSpace(`
Pitool is a tool for interacting with a pilosa server.

Usage:

	pitool command [arguments]

The commands are:

	create     create pilosa clusters
	bagent     run a benchmarking agent
	bspawn     create a cluster and agents and run benchmarks based on config file

Use the "-h" flag with any command for more information.
`)
}

// Run executes the main program execution.
func (m *Main) Run() error { return m.Cmd.Run(context.Background()) }

// ParseFlags parses command line flags from args.
func (m *Main) ParseFlags(args []string) error {
	var command string
	if len(args) > 0 {
		command = args[0]
		args = args[1:]
	}

	switch command {
	case "", "help", "-h":
		fmt.Fprintln(m.Stderr, m.Usage())
		fmt.Fprintln(m.Stderr, "")
		return flag.ErrHelp
	case "create":
		m.Cmd = NewCreateCommand(m.Stdin, m.Stdout, m.Stderr)
	case "bagent":
		m.Cmd = NewBagentCommand(m.Stdin, m.Stdout, m.Stderr)
	case "bspawn":
		m.Cmd = NewBspawnCommand(m.Stdin, m.Stdout, m.Stderr)
	default:
		return ErrUnknownCommand
	}

	// Parse command's flags.
	if err := m.Cmd.ParseFlags(args); err == flag.ErrHelp {
		fmt.Fprintln(m.Stderr, m.Cmd.Usage())
		fmt.Fprintln(m.Stderr, "")
		return err
	} else if err != nil {
		return err
	}

	return nil
}

// Command represents an executable subcommand.
type Command interface {
	Usage() string
	ParseFlags(args []string) error
	Run(context.Context) error
}

// CreateCommand represents a command for creating a pilosa cluster.
type CreateCommand struct {
	ServerN       int      `json:"serverN"`
	ReplicaN      int      `json:"replicaN"`
	LogFilePrefix string   `json:"log-file-prefix"`
	Hosts         []string `json:"hosts"`
	GoMaxProcs    int      `json:"gomaxprocs"`

	SSHUser string `json:"ssh-user"`

	CopyBinary bool   `json:"copy-binary"`
	GOOS       string `json:"goos"`
	GOARCH     string `json:"goarch"`

	// The following are set by CreateCommand.Run once they are known and exist
	// to appear in the output
	LogFiles   []string `json:"log-files"`
	FinalHosts []string `json:"final-hosts"`

	// Standard input/output
	Stdin  io.Reader `json:"-"`
	Stdout io.Writer `json:"-"`
	Stderr io.Writer `json:"-"`
}

// NewCreateCommand returns a new instance of CreateCommand.
func NewCreateCommand(stdin io.Reader, stdout, stderr io.Writer) *CreateCommand {
	return &CreateCommand{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
	}
}

// ParseFlags parses command line flags from args.
func (cmd *CreateCommand) ParseFlags(args []string) error {
	fs := flag.NewFlagSet("pitool", flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)
	fs.IntVar(&cmd.ServerN, "serverN", 3, "")
	fs.IntVar(&cmd.ReplicaN, "replicaN", 1, "")
	fs.StringVar(&cmd.LogFilePrefix, "log-file-prefix", "", "")
	var hosts string
	fs.IntVar(&cmd.GoMaxProcs, "gomaxprocs", 0, "")
	fs.StringVar(&hosts, "hosts", "", "")
	fs.StringVar(&cmd.SSHUser, "ssh-user", "", "")
	fs.BoolVar(&cmd.CopyBinary, "copy-binary", false, "")
	fs.StringVar(&cmd.GOOS, "goos", "linux", "")
	fs.StringVar(&cmd.GOARCH, "goarch", "amd64", "")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if len(fs.Args()) > 0 {
		fmt.Fprintf(cmd.Stderr, "Uknown args: %v\n", strings.Join(fs.Args(), " "))
		return flag.ErrHelp
	}
	cmd.Hosts = customSplit(hosts, ",")
	return nil
}

// Usage returns the usage message to be printed.
func (cmd *CreateCommand) Usage() string {
	return strings.TrimSpace(`
usage: pitool create [args]

Creates a pilosa cluster. Defaults to a 3-node in-process cluster.

The following flags are allowed:

	-serverN
		number of hosts in cluster. Disregarded if 'hosts' is set

	-replicaN
		replication factor for cluster

	-hosts
		comma separated host:port list. If hosts is set, create will start
		pilosa on these pre-existing hosts. The same host may be listed multiple
		times with different ports

	-log-file-prefix
		output from the started cluster will go into files with
		this prefix (one per node)

	-ssh-user
		username to use when contacting remote hosts

	-gomaxprocs
		when starting a cluster on remote hosts, this will set the value
		of GOMAXPROCS.

	-copy-binary
		controls whether or not to build and copy pilosa to hosts

	-goos
		when using copy-binary, GOOS to use while building binary

	-goarch
		when using copy-binary, GOARCH to use while building binary
`)
}

// Run executes cluster creation.
func (cmd *CreateCommand) Run(ctx context.Context) error {
	var clus creator.Cluster
	if len(cmd.Hosts) == 0 {
		fmt.Fprintf(cmd.Stderr, "create: no hosts specified - creating cluster in-process\n")
		clus = &creator.LocalCluster{
			ReplicaN: cmd.ReplicaN,
			ServerN:  cmd.ServerN,
		}
	} else {
		if cmd.ServerN != 0 {
			fmt.Fprintf(cmd.Stderr, "create: hosts were specified, so ignoring serverN\n")
		}
		clus = &creator.RemoteCluster{
			ClusterHosts: cmd.Hosts,
			ReplicaN:     cmd.ReplicaN,
			SSHUser:      cmd.SSHUser,
			Stderr:       cmd.Stderr,
			GoMaxProcs:   cmd.GoMaxProcs,
			CopyBinary:   cmd.CopyBinary,
			GOOS:         cmd.GOOS,
			GOARCH:       cmd.GOARCH,
		}

	}

	err := clus.Start()
	if err != nil {
		return fmt.Errorf("starting cluster: %v", err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			fmt.Fprintf(cmd.Stderr, "\ncreate: caught signal - shutting down\n")
			err := clus.Shutdown()
			code := 0
			if err != nil {
				code = 1
			}
			os.Exit(code)
		}
	}()

	defer clus.Shutdown()
	cmd.FinalHosts = clus.Hosts()

	logReaders := clus.Logs()
	if cmd.LogFilePrefix != "" {
		cmd.LogFiles = make([]string, len(clus.Hosts()))
	}
	for i, _ := range clus.Hosts() {
		var f io.Writer = cmd.Stderr
		var err error
		if cmd.LogFilePrefix != "" {
			f, err = os.Create(cmd.LogFilePrefix + strconv.Itoa(i))
			if err != nil {
				return err
			}
			cmd.LogFiles[i] = f.(*os.File).Name()
		}

		go func(i int, f io.Writer) {
			_, err := io.Copy(f, logReaders[i])
			if err != nil {
				fmt.Fprintf(cmd.Stderr, "create: error copying cluster logs: '%v'\n", err)
			}
		}(i, f)
	}

	enc := json.NewEncoder(cmd.Stdout)
	enc.SetIndent("", "  ")
	err = enc.Encode(cmd)
	if err != nil {
		return err
	}
	fmt.Fprintf(cmd.Stderr, "create: cluster started.\n")
	select {}

}

// BagentCommand represents a command for running a benchmark agent. A benchmark
// agent runs multiple benchmarks in series in the order that they are specified
// on the command line.
type BagentCommand struct {
	// Slice of Benchmarks which will be run serially.
	Benchmarks []bench.Benchmark `json:"benchmarks"`
	// AgentNum will be passed to each benchmark's Run method so that it can
	// parameterize its behavior.
	AgentNum int `json:"agent-num"`

	// Enable pretty printing of results, for human consumption.
	HumanReadable bool `json:"human-readable"`

	// Slice of pilosa hosts to run the Benchmarks against.
	Hosts []string `json:"hosts"`

	Stdin  io.Reader `json:"-"`
	Stdout io.Writer `json:"-"`
	Stderr io.Writer `json:"-"`
}

// NewBagentCommand returns a new instance of BagentCommand.
func NewBagentCommand(stdin io.Reader, stdout, stderr io.Writer) *BagentCommand {
	return &BagentCommand{
		Benchmarks:    []bench.Benchmark{},
		Hosts:         []string{},
		AgentNum:      0,
		HumanReadable: false,

		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
	}
}

// ParseFlags parses command line flags for the BagentCommand. First the command
// wide flags `hosts` and `agent-num` are parsed. The rest of the flags should be
// a series of subcommands along with their flags. ParseFlags runs each
// subcommand's `ConsumeFlags` method which parses the flags for that command
// and returns the rest of the argument slice which should contain further
// subcommands.
func (cmd *BagentCommand) ParseFlags(args []string) error {
	fs := flag.NewFlagSet("pitool", flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)

	var pilosaHosts string
	fs.StringVar(&pilosaHosts, "hosts", "localhost:10101", "")
	fs.IntVar(&cmd.AgentNum, "agent-num", 0, "")
	fs.BoolVar(&cmd.HumanReadable, "human", true, "")

	if err := fs.Parse(args); err != nil {
		return err
	}
	remArgs := fs.Args()
	if len(remArgs) == 0 {
		return flag.ErrHelp
	}
	for len(remArgs) > 0 {
		var bm bench.Command
		var err error
		switch remArgs[0] {
		case "-help", "-h":
			return flag.ErrHelp
		case "diagonal-set-bits":
			bm = &bench.DiagonalSetBits{}
		case "random-set-bits":
			bm = &bench.RandomSetBits{}
		case "zipf":
			bm = &bench.Zipf{}
		case "multi-index-set-bits":
			bm = &bench.MultiIndexSetBits{}
		case "import":
			bm = bench.NewImport(cmd.Stdin, cmd.Stdout, cmd.Stderr)
		case "import-zipf":
			bm = bench.NewImportZipf(cmd.Stdin, cmd.Stdout, cmd.Stderr)
		case "slice-height":
			bm = bench.NewSliceHeight(cmd.Stdin, cmd.Stdout, cmd.Stderr)
		default:
			return fmt.Errorf("Unknown benchmark cmd: %v", remArgs[0])
		}
		remArgs, err = bm.ConsumeFlags(remArgs[1:])
		cmd.Benchmarks = append(cmd.Benchmarks, bm)
		if err != nil {
			if err == flag.ErrHelp {
				fmt.Fprintln(cmd.Stderr, bm.Usage())
				return fmt.Errorf("")
			}
			return fmt.Errorf("BagentCommand.ParseFlags: %v", err)
		}
	}
	cmd.Hosts = customSplit(pilosaHosts, ",")

	return nil
}

// Usage returns the usage message to be printed.
func (cmd *BagentCommand) Usage() string {
	return strings.TrimSpace(`
usage: pitool bagent [options] <subcommand [options]>...

Runs benchmarks against a pilosa cluster.

The following flags are allowed:

	-hosts ("localhost:10101")
		comma separated list of host:port describing all hosts in the cluster

	-agent-num (0)
		an integer differentiating this agent from others in the fleet

	-human (true)
		boolean to enable human-readable format

	subcommands:
		diagonal-set-bits
		random-set-bits
		zipf
		multi-index-set-bits
		import
		import-zipf
		slice-height
`)
}

// Run executes the benchmark agent.
func (cmd *BagentCommand) Run(ctx context.Context) error {
	sbm := serial(cmd.Benchmarks...)
	err := sbm.Init(cmd.Hosts, cmd.AgentNum)
	if err != nil {
		return fmt.Errorf("in cmd.Run initialization: %v", err)
	}

	res := sbm.Run(ctx)
	res["agent-num"] = cmd.AgentNum
	enc := json.NewEncoder(cmd.Stdout)
	if cmd.HumanReadable {
		enc.SetIndent("", "  ")
		res = bench.Prettify(res)
	}
	err = enc.Encode(res)
	if err != nil {
		fmt.Fprintln(cmd.Stderr, err)
	}

	return nil
}

// BspawnCommand represents a command for spawning complex benchmarks. This
// includes cluster creation and teardown, agent creation and teardown, running
// multiple benchmarks in series and/or parallel, and collecting all the
// results.
type BspawnCommand struct {
	// If PilosaHosts is specified, CreatorArgs is ignored and the existing
	// cluster specified here is used.
	PilosaHosts []string `json:"pilosa-hosts"`

	// CreateCommand will be used with these arguments to create a cluster -
	// the cluster will be used to populate the PilosaHosts field. This
	// should include everything that comes after `pitool create`
	CreatorArgs []string `json:"creator-args"`

	// List of hosts to run agents on. If this is empty, agents will be run
	// locally.
	AgentHosts []string `json:"agent-hosts"`

	// Makes output human readable
	HumanReadable bool `json:"human-readable"`

	// Result destination, ["stdout", "s3"]
	Output string `json:"output"`

	// If this is true, build and copy pitool binary to agent hosts.
	CopyBinary bool   `json:"copy-binary"`
	GOOS       string `json:"goos"`
	GOARCH     string `json:"goarch"`

	// Benchmarks is a slice of Spawns which specifies all of the bagent
	// commands to run. These will all be run in parallel, started on each
	// of the agents in a round robin fashion.
	Benchmarks []Spawn `json:"benchmarks"`

	SSHUser string `json:"ssh-user"`

	Stdin  io.Reader `json:"-"`
	Stdout io.Writer `json:"-"`
	Stderr io.Writer `json:"-"`
}

// Spawn represents a bagent command run in parallel across Num agents. The
// bagent command can run multiple Benchmarks serially within itself.
type Spawn struct {
	Num  int      `json:"num"`  // number of agents to run
	Name string   `json:"name"` // Should describe what this Spawn does
	Args []string `json:"args"` // everything that comes after `pitool bagent [arguments]`
}

// NewBspawnCommand returns a new instance of BspawnCommand.
func NewBspawnCommand(stdin io.Reader, stdout, stderr io.Writer) *BspawnCommand {
	return &BspawnCommand{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
	}
}

// ParseFlags parses command line flags from args.
func (cmd *BspawnCommand) ParseFlags(args []string) error {
	fs := flag.NewFlagSet("pitool", flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)
	creatorHosts := fs.String("creator.hosts", "", "")
	creatorLFP := fs.String("creator.log-file-prefix", "", "")
	creatorCopyBinary := fs.Bool("creator.copy-binary", false, "")
	pilosaHosts := fs.String("pilosa-hosts", "", "")
	agentHosts := fs.String("agent-hosts", "", "")
	sshUser := fs.String("ssh-user", "", "")
	fs.BoolVar(&cmd.HumanReadable, "human", true, "")
	fs.StringVar(&cmd.Output, "output", "stdout", "")
	fs.BoolVar(&cmd.CopyBinary, "copy-binary", false, "")
	fs.StringVar(&cmd.GOOS, "goos", "linux", "")
	fs.StringVar(&cmd.GOARCH, "goarch", "amd64", "")

	err := fs.Parse(args)
	if err != nil {
		return err
	}
	if len(fs.Args()) != 1 {
		return flag.ErrHelp
	}
	f, err := os.Open(fs.Args()[0])
	if err != nil {
		return err
	}
	dec := json.NewDecoder(f)
	err = dec.Decode(cmd)
	if err != nil {
		return err
	}
	if *pilosaHosts != "" {
		cmd.PilosaHosts = customSplit(*pilosaHosts, ",")
	}
	if *agentHosts != "" {
		cmd.AgentHosts = customSplit(*agentHosts, ",")
	}
	if *sshUser != "" {
		cmd.SSHUser = *sshUser
	}
	if *pilosaHosts == "" {
		cmd.CreatorArgs = []string{"-hosts=" + *creatorHosts, "-log-file-prefix=" + *creatorLFP, "-ssh-user=" + cmd.SSHUser, "-goos=" + cmd.GOOS, "-goarch=" + cmd.GOARCH}
		if *creatorCopyBinary {
			cmd.CreatorArgs = append(cmd.CreatorArgs, "-copy-binary")
		}
	}

	return nil
}

// Usage returns the usage message to be printed.
func (cmd *BspawnCommand) Usage() string {
	return strings.TrimSpace(`
usage: pitool spawn [flags] <configfile>

Benchmark orchestration tool - runs 'create' and potentially multiple instances
of 'bagent' spread across a number of hosts.

The following flags are allowed and will override the values in the config file:

	-creator.hosts ()
		hosts argument for pitool create

	-creator.log-file-prefix ()
		log-file-prefix argument for pitool create. If empty, log to stderr.

	-creator.copy-binary (false)
		pitool create should build and copy pilosa binary to cluster

	-pilosa-hosts ([])
		pilosa hosts to run against (will ignore creator args)

	-agent-hosts ("localhost")
		hosts to use for benchmark agents

	-ssh-user (current username)
		username to use when contacting remote hosts

	-human (true)
		toggle human readable output (indented json)

	-output ("stdout")
		string to select output destination, "stdout" or "s3"

	-copy-binary (false)
		controls whether or not to build and copy pitool to agents

	-goos (linux)
		when using copy-binary, GOOS to use while building binary

	-goarch (amd64)
		when using copy-binary, GOARCH to use while building binary


`)
}

// Run executes the main program execution.
func (cmd *BspawnCommand) Run(ctx context.Context) error {
	runUUID := uuid.NewV1()
	output := make(map[string]interface{})
	output["run-uuid"] = runUUID.String()
	if len(cmd.PilosaHosts) == 0 {
		fmt.Fprintln(cmd.Stderr, "bspawn: pilosa-hosts not specified - using create command to build cluster")
		r, w := io.Pipe()
		createCmd := NewCreateCommand(cmd.Stdin, w, cmd.Stderr)
		err := createCmd.ParseFlags(cmd.CreatorArgs)
		if err != nil {
			return err
		}
		go func() {
			err := createCmd.Run(ctx)
			if err != nil {
				fmt.Fprintf(cmd.Stderr, "bspawn: cluster creation error while spawning: %v\n", err)
			}
		}()
		clus := &CreateCommand{}
		dec := json.NewDecoder(r)
		err = dec.Decode(clus)
		if err != nil {
			return err
		}
		cmd.PilosaHosts = clus.FinalHosts
		output["cluster"] = clus
	}
	if len(cmd.AgentHosts) == 0 {
		fmt.Fprintln(cmd.Stderr, "bpspawn: no agent-hosts specified; all agents will be spawned on localhost")
		cmd.AgentHosts = []string{"localhost"}
	}
	output["spawn"] = cmd
	res, err := cmd.spawnRemote(ctx)
	if err != nil {
		return err
	}
	output["results"] = res

	var writer io.Writer
	if cmd.Output == "s3" {
		writer = bench.NewS3Uploader("benchmarks-pilosa", runUUID.String()+".json")
	} else if cmd.Output == "stdout" {
		writer = cmd.Stdout
	} else {
		return fmt.Errorf("invalid bspawn output destination")
	}

	enc := json.NewEncoder(writer)
	if cmd.HumanReadable {
		enc.SetIndent("", "  ")
		output = bench.Prettify(output)
	}
	return enc.Encode(output)
}

func (cmd *BspawnCommand) spawnRemote(ctx context.Context) (map[string]interface{}, error) {
	agentIdx := 0
	agentFleet, err := pssh.NewFleet(cmd.AgentHosts, cmd.SSHUser, "", cmd.Stderr)
	if err != nil {
		return nil, err
	}

	cmdName := "pitool"
	if cmd.CopyBinary {
		cmdName = "/tmp/pitool" + strconv.Itoa(rand.Int())
		fmt.Fprintf(cmd.Stderr, "bspawn: building pitool binary with GOOS=%v and GOARCH=%v to copy to agents at %v\n", cmd.GOOS, cmd.GOARCH, cmdName)
		pkg := "github.com/pilosa/tools/cmd/pitool"
		bin, err := build.Binary(pkg, cmd.GOOS, cmd.GOARCH)
		if err != nil {
			return nil, err
		}
		err = agentFleet.WriteFile(cmdName, "+x", bin)
		if err != nil {
			return nil, err
		}
	}

	sessions := make([]*ssh.Session, 0)
	results := make(map[string]interface{})
	resLock := sync.Mutex{}
	wg := sync.WaitGroup{}
	fmt.Fprintln(cmd.Stderr, "bspawn: running benchmarks")
	for _, sp := range cmd.Benchmarks {
		results[sp.Name] = make(map[int]interface{})
		for i := 0; i < sp.Num; i++ {
			agentIdx %= len(cmd.AgentHosts)
			sess, err := agentFleet[cmd.AgentHosts[agentIdx]].NewSession()
			if err != nil {
				return nil, err
			}
			agentIdx += 1
			sessions = append(sessions, sess)
			stdout, err := sess.StdoutPipe()
			if err != nil {
				return nil, err
			}
			wg.Add(1)
			go func(stdout io.Reader, name string, num int) {
				defer wg.Done()
				dec := json.NewDecoder(stdout)
				var v interface{}
				err := dec.Decode(&v)
				if err != nil {
					fmt.Fprintf(cmd.Stderr, "error decoding json: %v, spawn: %v\n", err, name)
				}
				resLock.Lock()
				results[name].(map[int]interface{})[num] = v
				resLock.Unlock()
			}(stdout, sp.Name, i)
			sess.Stderr = cmd.Stderr
			err = sess.Start(cmdName + " bagent -agent-num=" + strconv.Itoa(i) + " -hosts=" + strings.Join(cmd.PilosaHosts, ",") + " " + strings.Join(sp.Args, " "))
			if err != nil {
				return nil, err
			}
		}
	}

	for _, sess := range sessions {
		err = sess.Wait()
		if err != nil {
			return nil, fmt.Errorf("error waiting for remote bagent: %v", err)
		}
	}
	wg.Wait()
	return results, nil
}

type serialBenchmark struct {
	benchmarkers []bench.Benchmark
}

// Init calls Init for each benchmark. If there are any errors, it will return a
// non-nil error value.
func (sb *serialBenchmark) Init(hosts []string, agentNum int) error {
	errors := make([]error, len(sb.benchmarkers))
	hadErr := false
	for i, b := range sb.benchmarkers {
		errors[i] = b.Init(hosts, agentNum)
		if errors[i] != nil {
			hadErr = true
		}
	}
	if hadErr {
		return fmt.Errorf("Had errs in serialBenchmark.Init: %v", errors)
	}
	return nil
}

// Run runs the serial benchmark and returns it's results in a nested map - the
// top level keys are the indices of each benchmark in the list of benchmarks,
// and the values are the results of each benchmark's Run method.
func (sb *serialBenchmark) Run(ctx context.Context) map[string]interface{} {
	benchmarks := make([]map[string]interface{}, len(sb.benchmarkers))
	results := map[string]interface{}{"benchmarks": benchmarks}

	total_start := time.Now()
	for i, b := range sb.benchmarkers {
		start := time.Now()
		output := b.Run(ctx)
		if _, ok := output["runtime"]; ok {
			panic(fmt.Sprintf("Benchmark %v added 'runtime' to its results", b))
		}
		output["runtime"] = time.Now().Sub(start)
		ret := map[string]interface{}{"output": output, "metadata": b}
		benchmarks[i] = ret
	}
	results["total_runtime"] = time.Now().Sub(total_start)
	return results
}

// serial takes a variable number of Benchmarks and returns a Benchmark
// which combines then and will run each serially.
func serial(bs ...bench.Benchmark) bench.Benchmark {
	return &serialBenchmark{
		benchmarkers: bs,
	}
}

func customSplit(s string, sep string) []string {
	if s == "" {
		return []string{}
	}
	return strings.Split(s, sep)
}
