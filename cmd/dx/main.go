package main

import (
	"fmt"
	"os"

	"github.com/pilosa/tools/dx"
)

func main() {
	if err := dx.NewRootCmd().Execute(); err != nil {
		fmt.Printf("%+v", err)
		os.Exit(1)
	}
}
