package cmd

import (
	"context"
	"fmt"
	"io"

	"github.com/pilosa/tools/bench"
	"github.com/spf13/cobra"
)

// NewSpawnCommand
func NewSpawnCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	spawn := bench.NewSpawnCommand(stdin, stdout, stderr)
	spawnCmd := &cobra.Command{
		Use:   "spawn",
		Short: "Execute a series of benchmarks from multiple agent hosts against a Pilosa cluster.",
		Long: `
The Spawn command takes a configuration file which specifies a series of
benchmarks to be run against a Pilosa cluster. Each of these benchmarks should
have a descriptive name, a "num" which is the number of agents to run, and the
actual arguments to give to "pi bench" which specify which benchmark to run.
Each agent running the benchmark will be given a unique and monotonic agent
number so that it may parameterize its behavior. Each benchmark command should
document how the agent number changes its behavior.

While the spawn command's behavior can be controlled via command line arguments,
it generally will compile the "pi" command for the target OS/arch, copy it to
the agents via ssh, run each specified benchmark, and then aggregate all the
results by reading the stdout of each agent benchmark.
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			err := spawn.Run(context.Background())
			if err != nil {
				return fmt.Errorf("running spawn: %v", err)
			}
			return nil
		},
	}

	flags := spawnCmd.Flags()
	flags.BoolVar(&spawn.HumanReadable, "human", true, "Make output human friendly.")
	flags.StringVar(&spawn.Output, "output", "stdout", "(stdout | s3). Write JSON results to stdout, or store them in S3 (requires AWS credentials to be setup).")
	flags.StringVar(&spawn.AWSRegion, "aws-region", "", "Which AWS region to use when output is s3. (defaults to AWS_REGION from environment or 'us-east-1' if that is blank)")
	flags.StringVar(&spawn.BucketName, "bucket-name", "benchmarks-pilosa", "S3 bucket name to write results to.")
	flags.BoolVar(&spawn.CopyBinary, "copy-binary", true, "Build and copy pi binary to agent hosts for executing benchmarks.")
	flags.StringVar(&spawn.GOOS, "goos", "", "Set GOOS for building binary. (defaults to local runtime's GOOS)")
	flags.StringVar(&spawn.GOARCH, "goarch", "amd64", "Set GOARCH for building binary. (defaults to local runtime's GOARCH)")
	flags.StringSliceVar(&spawn.PilosaHosts, "pilosa-hosts", []string{"localhost:10101"}, "Comma separated host:port of Pilosa cluster.")
	flags.StringSliceVar(&spawn.AgentHosts, "agent-hosts", []string{"localhost"}, "Comma separated hostnames of agents.")
	flags.StringVar(&spawn.SSHUser, "ssh-user", "", "SSH username for remote hosts. (defaults to current user)")
	flags.StringVar(&spawn.SpawnFile, "spawn-file", "", "JSON file defining spawn benchmarks. (required)")

	return spawnCmd
}

func init() {
	subcommandFns["spawn"] = NewSpawnCommand
}
