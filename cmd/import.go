package cmd

import (
	"context"
	"io"

	"github.com/pilosa/tools/bench"
	"github.com/spf13/cobra"
)

// NewBenchCommand subcommands
func NewImportCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	importer := &bench.Import{}
	importCmd := &cobra.Command{
		Use:   "import",
		Short: "Import random data into Pilosa quickly.",
		Long:  `import generates random data which can be controlled by command line flags and streams it into Pilosa's /import endpoint. Agent num has no effect`,
		RunE: func(cmd *cobra.Command, args []string) error {
			flags := cmd.Flags()
			hostSetup, err := bench.HostSetupFromFlags(flags)
			if err != nil {
				return err
			}
			agentNum, err := flags.GetInt("agent-num")
			if err != nil {
				return err
			}
			result := bench.RunBenchmark(context.Background(), hostSetup, agentNum, importer)
			err = PrintResults(cmd, result, stdout)
			if err != nil {
				return err
			}
			return nil
		},
	}

	flags := importCmd.Flags()
	flags.Int64Var(&importer.MinRowID, "min-row-id", 0, "Minimum row id of set bits.")
	flags.Int64Var(&importer.MinColumnID, "min-column-id", 0, "Minimum column id of set bits.")
	flags.Int64Var(&importer.MaxRowID, "max-row-id", 1000, "Maximum row id of set bits.")
	flags.Int64Var(&importer.MaxColumnID, "max-column-id", 1000, "Maximum column id of set bits.")
	flags.Int64Var(&importer.Iterations, "iterations", 100000, "Number of bits to set")
	flags.Int64Var(&importer.Seed, "seed", 0, "Random seed.")
	flags.StringVar(&importer.Index, "index", defaultIndex, "Pilosa index in which to set bits.")
	flags.StringVar(&importer.Frame, "frame", defaultFrame, "Pilosa frame in which to set bits.")
	flags.StringVar(&importer.Distribution, "distribution", "exponential", "Random distribution for deltas between set bits (exponential or uniform).")
	flags.UintVar(&importer.BufferSize, "buffer-size", 10000000, "Number of set bits to buffer in importer before POSTing to Pilosa.")

	return importCmd
}

func init() {
	benchCommandFns["import"] = NewImportCommand
}
