package main

import (
	"fmt"
	"os"

	dx "github.com/pilosa/tools/dx/cmd"
)

func main() {
	if err := dx.NewRootCmd().Execute(); err != nil {
		fmt.Printf("%+v", err)
		os.Exit(1)
	}
}
