package dx

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	// "github.com/pilosa/tools/dx"
)

// flags
var ThreadCount int
var CHosts []string
var PHosts []string
var CPort int
var PPort int


func main() {
	if err := NewRootCmd().Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// NewRootCmd creates an instance of the cobra root command for dx.
func NewRootCmd() *cobra.Command {
	rc := &cobra.Command{
		Use:   "dx",
		Short: "compare changes between two Pilosa instances",
		Long:  `Compare changes between candidate Pilosa instance and last known-good Pilosa version`,
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("dx tool")
		},
	}
	rc.PersistentFlags().IntVarP(&ThreadCount, "threadCount", "r", 1, "Number of concurrent goroutines to allocate")
	rc.PersistentFlags().StringSliceVar(&CHosts ,"cHosts", []string{"localhost"}, "Hosts of candidate instance")
	rc.PersistentFlags().StringSliceVar(&PHosts ,"pHosts", []string{"localhost"}, "Hosts of primary instance")
	rc.PersistentFlags().IntVar(&CPort, "cPort", 10101, "Port of candidate instance")
	rc.PersistentFlags().IntVar(&PPort, "pPort", 10101, "Port of primary instance")
	
	rc.AddCommand(NewIngestCommand())
	rc.AddCommand(NewQueryCommand())
	rc.AddCommand(NewAllCommand())

	rc.SetOutput(os.Stderr)

	return rc
}