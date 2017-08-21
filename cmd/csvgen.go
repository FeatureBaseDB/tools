package cmd

import (
	"io"

	"github.com/pilosa/tools/csvgen"
	"github.com/spf13/cobra"
)

// NewCreateCommand
func NewCSVGenCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	gen := &csvgen.Main{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
	}
	genCmd := &cobra.Command{
		Use:   "csvgen",
		Short: "Generate CSV files for import into Pilosa and other tools.",
		Long: `

`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return gen.Run()
		},
	}

	flags := genCmd.Flags()
	flags.StringVarP(&gen.Filename, "filename", "f", "data.csv", "File to write to (will be truncated).")
	flags.IntVar(&gen.RunsMin, "runs-min", 2, "Minimum number of runs per container")
	flags.IntVar(&gen.RunsMax, "runs-max", 20000, "Maximum number of runs per container")
	flags.IntVar(&gen.NMin, "n-min", 100, "Minimum number of bits per container")
	flags.IntVar(&gen.NMax, "n-max", 60000, "Maximum number of bits per container")
	flags.Uint64Var(&gen.Rows, "rows", 10, "Number of rows in generated data.")
	flags.Uint64Var(&gen.Cols, "cols", 65536, "Number of columns in generated data. Rounded up to nearest multiple of s^16.")

	return genCmd
}

func init() {
	subcommandFns["csvgen"] = NewCSVGenCommand
}
