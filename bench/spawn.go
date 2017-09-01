package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"time"

	"github.com/pilosa/tools"
	"github.com/pilosa/tools/build"
	pssh "github.com/pilosa/tools/ssh"
	"github.com/satori/go.uuid"
	"golang.org/x/crypto/ssh"
	"net/http"
	url2 "net/url"
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
	Output     string `json:"output"`
	BucketName string `json:"bucket-name"`
	AWSRegion  string `json:"aws-region"`

	// If this is true, build and copy pi binary to agent hosts.
	CopyBinary bool   `json:"copy-binary"`
	GOOS       string `json:"goos"`
	GOARCH     string `json:"goarch"`

	SpawnFile string `json:"spawn-file"`
	FileName  string `json:"filename"`
	Repo      string `json:"repo"`

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

	if cmd.SpawnFile == "" && cmd.FileName == "" {
		return fmt.Errorf("a spawn file must be specified")
	}
	var f *os.File
	var err error
	var gitContent *GitContent
	if cmd.FileName != "" {
		out, err := ioutil.TempFile("", "tmp")
		if err != nil {
			return err
		}
		defer os.Remove(out.Name())

		gitContent, err = cmd.getGithubContent(ctx)
		if err != nil {
			return err
		}
		err = cmd.downloadFile(gitContent.DownloadURL, out)
		if err != nil {
			return err
		}

		f, err = os.Open(out.Name())
		if err != nil {
			return fmt.Errorf("trying to open spawn file: %v", err)
		}
	} else {
		f, err = os.Open(cmd.SpawnFile)
		if err != nil {
			return fmt.Errorf("trying to open spawn file: %v", err)
		}
	}

	dec := json.NewDecoder(f)
	err = dec.Decode(cmd)
	if err != nil {
		return fmt.Errorf("decoding spawn file: %v", err)
	}
	fmt.Println(cmd.BucketName)

	runUUID := uuid.NewV1()
	if len(cmd.PilosaHosts) == 0 {
		return fmt.Errorf("spawn: pilosa-hosts not specified")
	}
	if len(cmd.AgentHosts) == 0 {
		return fmt.Errorf("spawn: no agent-hosts specified - specify at least one agent host (localhost is acceptable)")
	}
	if cmd.GOOS == "" {
		cmd.GOOS = runtime.GOOS
	}
	if cmd.GOARCH == "" {
		cmd.GOARCH = runtime.GOARCH
	}
	if cmd.AWSRegion == "" {
		cmd.AWSRegion = os.Getenv("AWS_REGION")
		if cmd.AWSRegion == "" {
			cmd.AWSRegion = "us-east-1"
		}
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

	if gitContent != nil {
		output.Version = gitContent.Sha
	}

	var writer io.Writer
	if cmd.Output == "s3" {
		writer = NewS3Uploader(cmd.BucketName, runUUID.String()+".json", cmd.AWSRegion)
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
	Version          string            `json:"version"`
}

// BenchmarkResult represents the result of a single benchmark run by
// potentially multiple agents. Each agent's result is represented in the slice
// of BenchResult objects.
type BenchmarkResult struct {
	BenchmarkName string   `json:"benchmark-name"`
	AgentResults  []Result `json:"agent-results"`
}

func (cmd *SpawnCommand) spawnRemote(ctx context.Context) ([]BenchmarkResult, error) {
	agentIdx := 0
	agentFleet, err := pssh.NewFleet(cmd.AgentHosts, cmd.SSHUser, "", cmd.Stderr)
	if err != nil {
		return nil, fmt.Errorf("creating fleet: %v", err)
	}

	cmdName := "pi"
	if cmd.CopyBinary {
		rand.Seed(time.Now().UnixNano())
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
			AgentResults:  make([]Result, bm.Num),
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
				benchResult := Result{}
				err := dec.Decode(&benchResult)
				if err != nil {
					errOut, err2 := ioutil.ReadAll(stderr)
					if err2 != nil {
						errOut = []byte(fmt.Sprintf("problem reading remote stderr: %v", err2))
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

// getGithubContent using github api return url of a file to download and commit hash
func (cmd *SpawnCommand) getGithubContent(ctx context.Context) (*GitContent, error) {
	fileName := cmd.FileName
	repo := cmd.Repo
	path, err := url2.Parse(repo)
	uri := path.Path
	gitURL := fmt.Sprintf("https://api.github.com/repos%s/contents/%s", uri, fileName)

	resp, err := http.Get(gitURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("getting file %v from %v, err: %v", fileName, repo, string(body))
	}
	var content GitContent
	err = json.NewDecoder(resp.Body).Decode(&content)
	if err != nil {
		return nil, err
	}
	if content.DownloadURL == "" {
		return nil, fmt.Errorf("No file to download")
	}
	return &content, nil
}

// downloadFile downloads git file and copy to temp file
func (cmd *SpawnCommand) downloadFile(url string, fileName *os.File) error {

	respFile, err := http.Get(url)
	if err != nil {
		return err
	}
	defer respFile.Body.Close()

	// Writer the body to file
	_, err = io.Copy(fileName, respFile.Body)
	if err != nil {
		return err
	}
	return nil
}

type GitContent struct {
	Sha         string `json:"sha"`
	DownloadURL string `json:"download_url"`
}
