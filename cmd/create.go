package cmd

import (
	"context"
	"io"

	"github.com/pilosa/tools/creator"
	"github.com/spf13/cobra"
)

// NewCreateCommand
func NewCreateCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	create := creator.NewCreateCommand(stdin, stdout, stderr)
	createCmd := &cobra.Command{
		Use:   "create",
		Short: "Create Pilosa clusters.",
		Long: `This tool is used to create Pilosa clusters.

One can create a cluster on a single host, using different ports,
or specify a list of hosts, and build and copy the Pilosa binary
to those hosts.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return create.Run(context.Background())
		},
	}

	flags := createCmd.Flags()
	flags.IntVar(&create.ServerN, "serverN", 3, "Number of hosts in the cluster to create.")
	flags.IntVar(&create.ReplicaN, "replicaN", 1, "Replication factor of created cluster.")
	flags.StringVar(&create.LogFilePrefix, "log-file-prefix", "", "A log file will be created for each host with this prefix.")
	flags.IntVar(&create.GoMaxProcs, "gomaxprocs", 0, "Set GOMAXPROCS on each host to control CPU usage.")
	flags.StringSliceVar(&create.Hosts, "hosts", []string{}, "List of hosts on which to start the cluster.")
	flags.StringVar(&create.SSHUser, "ssh-user", "", "SSH username for connecting to remote hosts.")
	flags.BoolVar(&create.CopyBinary, "copy-binary", false, "Set to true to build and copy the Pilosa binary to remote hosts.")
	flags.StringVar(&create.GOOS, "goos", "linux", "GOOS to use if copy-binary is set.")
	flags.StringVar(&create.GOARCH, "goarch", "amd64", "GOARCH to use if copy-binary is set.")

	return createCmd
}

func init() {
	subcommandFns["create"] = NewCreateCommand
}
