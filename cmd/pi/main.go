/*
This is the entrypoint for the pi binary.
*/
package main

import (
	"fmt"
	"os"

	"github.com/pilosa/tools/cmd"
)

func main() {
	rootCmd := cmd.NewRootCommand(os.Stdin, os.Stdout, os.Stderr)
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
