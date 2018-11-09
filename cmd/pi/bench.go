package main

import (
	"encoding/json"
	"io"

	"github.com/pilosa/tools/bench"
	"github.com/spf13/cobra"
)

const (
	defaultIndex      = "ibench"
	defaultField      = "fbench"
	defaultRangeField = "range-field"
)

func NewBenchCommand() *cobra.Command {
	benchCmd := &cobra.Command{
		Use:   "bench",
		Short: "Runs benchmarks against a pilosa cluster.",
		Long: `Runs benchmarks against a pilosa cluster.

See the various subcommands for specific benchmarks and their arguments. The
various benchmarks should modulate their behavior based on what agent-num is
given, so that multiple benchmarks with identical configurations but differing
agent numbers will do interesting work.

`,
	}

	flags := benchCmd.PersistentFlags()
	flags.StringSlice("hosts", []string{"localhost:10101"}, "Comma separated list of \"host:port\" pairs of the Pilosa cluster.")
	flags.Int("agent-num", 0, "A unique integer to associate with this invocation of 'bench' to distinguish it from others running concurrently.")
	flags.Bool("human", true, "Make output human friendly.")
	flags.Bool("tls.skip-verify", false, "Skip TLS certificate verification (not secure)")

	benchCmd.AddCommand(NewBasicQueryCommand())
	benchCmd.AddCommand(NewDiagonalSetBitsCommand())
	benchCmd.AddCommand(NewImportCommand())
	benchCmd.AddCommand(NewImportRangeCommand())
	benchCmd.AddCommand(NewQueryCommand())
	benchCmd.AddCommand(NewRandomQueryCommand())
	benchCmd.AddCommand(NewRandomSetCommand())
	benchCmd.AddCommand(NewRangeQueryCommand())
	benchCmd.AddCommand(NewSliceWidthCommand())
	benchCmd.AddCommand(NewZipfCommand())

	return benchCmd
}

// PrintResults encodes the output of a benchmark subcommand as json and writes
// it to the given Writer. It takes the "human" flag into account when encoding
// the json.
func PrintResults(cmd *cobra.Command, result *bench.Result, out io.Writer) error {
	human, err := cmd.Flags().GetBool("human")
	if err != nil {
		return err
	}

	enc := json.NewEncoder(out)
	if human {
		enc.SetIndent("", "  ")
	}
	if err := enc.Encode(result); err != nil {
		return err
	}
	return nil
}
