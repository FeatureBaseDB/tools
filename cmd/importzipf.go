package cmd

import (
	"context"
	"io"

	"github.com/pilosa/tools/bench"
	"github.com/spf13/cobra"
)

// NewBenchCommand subcommands
func NewImportZipfCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	importZipf := bench.NewImportZipf(stdin, stdout, stderr)
	importZipfCmd := &cobra.Command{
		Use:   "import-zipf",
		Short: "Import random data into Pilosa quickly.",
		Long: `import-zipf generates an import file which sets bits according to the Zipf
distribution in both row and column id and imports using pilosa's bulk import
interface. Agent num has no effect`,
		RunE: func(cmd *cobra.Command, args []string) error {
			flags := cmd.Flags()
			hosts, err := flags.GetStringSlice("hosts")
			if err != nil {
				return err
			}
			agentNum, err := flags.GetInt("agent-num")
			if err != nil {
				return err
			}
			result := bench.RunBenchmark(context.Background(), hosts, agentNum, importZipf)
			err = PrintResults(cmd, result, stdout)
			if err != nil {
				return err
			}
			return nil
		},
	}

	flags := importZipfCmd.Flags()
	flags.Int64Var(&importZipf.BaseBitmapID, "base-bitmap-id", 0, "Minimum row id of set bits.")
	flags.Int64Var(&importZipf.BaseProfileID, "base-profile-id", 0, "Minimum column id of set bits.")
	flags.Int64Var(&importZipf.MaxBitmapID, "max-bitmap-id", 1000, "Maximum row id of set bits.")
	flags.Int64Var(&importZipf.MaxProfileID, "max-profile-id", 1000, "Maximum column id of set bits.")
	flags.IntVar(&importZipf.Iterations, "iterations", 100000, "Number of bits to set")
	flags.Int64Var(&importZipf.Seed, "seed", 0, "Random seed.")
	flags.StringVar(&importZipf.Index, "index", "benchindex", "Pilosa index in which to set bits.")
	flags.StringVar(&importZipf.Frame, "frame", "frame", "Pilosa frame in which to set bits.")
	flags.IntVar(&importZipf.BufferSize, "buffer-size", 10000000, "Number of set bits to buffer in importer before POSTing to Pilosa.")

	return importZipfCmd
}

func init() {
	benchCommandFns["import-zipf"] = NewImportZipfCommand
}
