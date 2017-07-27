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
		Short: "Spawn benchmarks.",
		Long:  `Spawn a fleet of 'agents' to execute benchmarks against a Pilosa cluster`,
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
	flags.StringVar(&spawn.Output, "output", "stdout", "Output to 'stdout' or 's3'.")
	flags.BoolVar(&spawn.CopyBinary, "copy-binary", false, "Build and copy pi binary to agent hosts for executing benchmarks?")
	flags.StringVar(&spawn.GOOS, "goos", "linux", "Set GOOS for building binary.")
	flags.StringVar(&spawn.GOARCH, "goarch", "amd64", "Set GOARCH for building binary.")
	flags.StringSliceVar(&spawn.PilosaHosts, "pilosa-hosts", []string{"localhost:10101"}, "Comma separated host:port of Pilosa cluster.")
	flags.StringSliceVar(&spawn.AgentHosts, "agent-hosts", []string{}, "Comma separated hostnames of agents.")
	flags.StringVar(&spawn.SSHUser, "ssh-user", "", "SSH username for remote hosts.")
	flags.StringVar(&spawn.SpawnFile, "spawn-file", "", "JSON file defining spawn benchmarks.")

	return spawnCmd
}

func init() {
	subcommandFns["spawn"] = NewSpawnCommand
}
