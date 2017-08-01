package cmd

import (
	"io"

	"github.com/pilosa/tools/bview"
	"github.com/spf13/cobra"
)

// NewCreateCommand
func NewBviewCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	viewer := &bview.Main{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
	}
	createCmd := &cobra.Command{
		Use:   "bview",
		Short: "View benchmarking results stored in an s3 bucket.",
		Long: `This lists all objects in an s3 bucket and tried to intperpret them as JSON benchmarking results.

It summarizes the benchmark results in a more palatable human readable format.
AWS credentials must be present in the local environment.
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return viewer.Run()
		},
	}

	flags := createCmd.Flags()
	flags.StringVarP(&viewer.Bucket, "bucket", "b", "benchmarks-pilosa", "S3 bucket name to list. You must have permissions.")

	return createCmd
}

func init() {
	subcommandFns["bview"] = NewBviewCommand
}
