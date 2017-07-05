// creator contains code for standing up pilosa clusters
package creator

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
)

type Cluster interface {
	Start() error
	Hosts() []string
	Shutdown() error
	Logs() []io.Reader
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

// Run executes cluster creation.
func (cmd *CreateCommand) Run(ctx context.Context) error {
	var clus Cluster
	if len(cmd.Hosts) == 0 {
		fmt.Fprintf(cmd.Stderr, "create: no hosts specified - creating cluster in-process\n")
		clus = &LocalCluster{
			ReplicaN: cmd.ReplicaN,
			ServerN:  cmd.ServerN,
		}
	} else {
		if cmd.ServerN != 0 {
			fmt.Fprintf(cmd.Stderr, "create: hosts were specified, so ignoring serverN\n")
		}
		clus = &RemoteCluster{
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
