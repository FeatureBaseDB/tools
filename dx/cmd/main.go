package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	// "github.com/pilosa/tools/dx"
)

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
	rc.PersistentFlags().IntP("threadCount", "r", 1, "Number of concurrent goroutines to allocate")
	rc.PersistentFlags().StringSliceP("candidate", "c", []string{"localhost:10102"}, "Address of candidate instance")
	rc.PersistentFlags().StringSliceP("primary", "p", []string{"localhost:10103"}, "Address of primary instance")
	
	rc.AddCommand(NewIngestCommand())
	rc.AddCommand(NewQueryCommand())
	rc.AddCommand(NewAllCommand())

	rc.SetOutput(os.Stderr)

	return rc
}