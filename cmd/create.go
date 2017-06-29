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
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			return create.Run(context.Background())
		},
	}

	flags := createCmd.Flags()
	flags.IntVar(&create.ServerN, "serverN", 3, "")
	flags.IntVar(&create.ReplicaN, "replicaN", 1, "")
	flags.StringVar(&create.LogFilePrefix, "log-file-prefix", "", "")
	flags.IntVar(&create.GoMaxProcs, "gomaxprocs", 0, "")
	flags.StringSliceVar(&create.Hosts, "hosts", []string{}, "")
	flags.StringVar(&create.SSHUser, "ssh-user", "", "")
	flags.BoolVar(&create.CopyBinary, "copy-binary", false, "")
	flags.StringVar(&create.GOOS, "goos", "linux", "")
	flags.StringVar(&create.GOARCH, "goarch", "amd64", "")

	return createCmd
}

func init() {
	subcommandFns["create"] = NewCreateCommand
}
