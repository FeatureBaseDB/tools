package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/pilosa/tools"
	"github.com/pilosa/tools/build"
	pssh "github.com/pilosa/tools/ssh"
	"github.com/satori/go.uuid"
	"golang.org/x/crypto/ssh"
)

// SpawnCommand represents a command for spawning complex benchmarks. This
// includes cluster creation and teardown, agent creation and teardown, running
// multiple benchmarks in series and/or parallel, and collecting all the
// results.
type SpawnCommand struct {
	// If PilosaHosts is specified, CreatorArgs is ignored and the existing
	// cluster specified here is used.
	PilosaHosts []string `json:"pilosa-hosts"`

	// List of hosts to run agents on. If this is empty, agents will be run
	// locally.
	AgentHosts []string `json:"agent-hosts"`

	// Makes output human readable
	HumanReadable bool `json:"human-readable"`

	// Result destination, ["stdout", "s3"]
	Output string `json:"output"`

	// If this is true, build and copy pi binary to agent hosts.
	CopyBinary bool   `json:"copy-binary"`
	GOOS       string `json:"goos"`
	GOARCH     string `json:"goarch"`

	SpawnFile string `json:"spawn-file"`

	// Benchmarks is a slice of Spawns which specifies all of the bench
	// commands to run. These will all be run in parallel, started on each
	// of the agents in a round robin fashion.
	Benchmarks []Spawn `json:"benchmarks"`

	SSHUser string `json:"ssh-user"`

	Stdin  io.Reader `json:"-"`
	Stdout io.Writer `json:"-"`
	Stderr io.Writer `json:"-"`
}

// Spawn represents a bench command run in parallel across Num agents.
type Spawn struct {
	Num  int      `json:"num"`  // number of agents to run
	Name string   `json:"name"` // Should describe what this Spawn does
	Args []string `json:"args"` // everything that comes after `pi bench [arguments]`
}

// NewSpawnCommand returns a new instance of SpawnCommand.
func NewSpawnCommand(stdin io.Reader, stdout, stderr io.Writer) *SpawnCommand {
	return &SpawnCommand{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
	}
}

// Run executes the main program execution.
func (cmd *SpawnCommand) Run(ctx context.Context) error {
	f, err := os.Open(cmd.SpawnFile)
	if err != nil {
		return fmt.Errorf("trying to open spawn file: %v", err)
	}
	dec := json.NewDecoder(f)
	err = dec.Decode(cmd)
	if err != nil {
		return err
	}

	runUUID := uuid.NewV1()
	if len(cmd.PilosaHosts) == 0 {
		return fmt.Errorf("spawn: pilosa-hosts not specified")
	}
	if len(cmd.AgentHosts) == 0 {
		fmt.Fprintln(cmd.Stderr, "spawn: no agent-hosts specified; all agents will be spawned on localhost")
		cmd.AgentHosts = []string{"localhost"}
	}
	res, err := cmd.spawnRemote(ctx)
	if err != nil {
		return fmt.Errorf("spawn remote: %v", err)
	}
	output := &SpawnResult{
		RunUUID:          runUUID.String(),
		PiVersion:        tools.Version,
		PiBuildTime:      tools.BuildTime,
		Configuration:    cmd,
		BenchmarkResults: res,
	}

	var writer io.Writer
	if cmd.Output == "s3" {
		writer = NewS3Uploader("benchmarks-pilosa", runUUID.String()+".json")
	} else if cmd.Output == "stdout" {
		writer = cmd.Stdout
	} else {
		return fmt.Errorf("invalid spawn output destination")
	}

	enc := json.NewEncoder(writer)
	if cmd.HumanReadable {
		enc.SetIndent("", "  ")
		PrettifySpawnResult(output)
	}
	return enc.Encode(output)
}

// SpawnResult represents the full JSON output of the spawn command.
type SpawnResult struct {
	RunUUID          string            `json:"run-uuid"`
	BenchmarkResults []BenchmarkResult `json:"benchmark-results"`
	PiVersion        string            `json:"pi-version"`
	PiBuildTime      string            `json:"pi-build-time"`
	Configuration    *SpawnCommand     `json:"configuration"`
}

// BenchmarkResult represents the result of a single benchmark run by
// potentially multiple agents. Each agent's result is represented in the slice
// of BenchResult objects.
type BenchmarkResult struct {
	BenchmarkName string        `json:"benchmark-name"`
	AgentResults  []BenchResult `json:"agent-results"`
}

func (cmd *SpawnCommand) spawnRemote(ctx context.Context) ([]BenchmarkResult, error) {
	agentIdx := 0
	agentFleet, err := pssh.NewFleet(cmd.AgentHosts, cmd.SSHUser, "", cmd.Stderr)
	if err != nil {
		return nil, fmt.Errorf("creating fleet: %v", err)
	}

	cmdName := "pi"
	if cmd.CopyBinary {
		cmdName = "/tmp/pi" + strconv.Itoa(rand.Int())
		fmt.Fprintf(cmd.Stderr, "spawn: building pi binary with GOOS=%v and GOARCH=%v to copy to agents at %v\n", cmd.GOOS, cmd.GOARCH, cmdName)
		pkg := "github.com/pilosa/tools/cmd/pi"
		bin, err := build.Binary(pkg, cmd.GOOS, cmd.GOARCH)
		if err != nil {
			return nil, fmt.Errorf("building binary: %v", err)
		}
		err = agentFleet.WriteFile(cmdName, "+x", bin)
		if err != nil {
			return nil, fmt.Errorf("writing binary to agents: %v", err)
		}
	}

	results := make([]BenchmarkResult, len(cmd.Benchmarks))
	fmt.Fprintln(cmd.Stderr, "spawn: running benchmarks")
	for bmNum, bm := range cmd.Benchmarks {
		sessions := make([]*ssh.Session, 0)
		wg := sync.WaitGroup{}
		results[bmNum] = BenchmarkResult{
			BenchmarkName: bm.Name,
			AgentResults:  make([]BenchResult, bm.Num),
		}
		for agentNum := 0; agentNum < bm.Num; agentNum++ {
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
			stderr, err := sess.StderrPipe()
			if err != nil {
				return nil, err
			}
			wg.Add(1)
			go func(stdout, stderr io.Reader, name string, bmNum, agentNum int) {
				defer wg.Done()
				dec := json.NewDecoder(stdout)
				benchResult := BenchResult{}
				err := dec.Decode(&benchResult)
				if err != nil {
					errOut, err := ioutil.ReadAll(stderr)
					if err != nil {
						errOut = []byte(fmt.Sprintf("problem reading remote stderr: %v", err))
					}
					fmt.Fprintf(cmd.Stderr, "error decoding json: %v, spawn: %v, stderr: %s\n", err, name, errOut)
				}
				results[bmNum].AgentResults[agentNum] = benchResult
			}(stdout, stderr, bm.Name, bmNum, agentNum)
			sess.Stderr = cmd.Stderr
			err = sess.Start(cmdName + " bench --agent-num=" + strconv.Itoa(agentNum) + " --hosts=" + strings.Join(cmd.PilosaHosts, ",") + " " + strings.Join(bm.Args, " "))
			if err != nil {
				return nil, err
			}
		}
		for _, sess := range sessions {
			err = sess.Wait()
			if err != nil {
				return nil, fmt.Errorf("error waiting for remote bench cmd: %v", err)
			}
		}
		wg.Wait()
	}

	return results, nil
}
