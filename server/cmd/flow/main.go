package main

import (
	"fmt"
	"os"

	"github.com/premchandkpc/FlowRulZ/server/internal/flow"
)

func main() {
	cli := flow.NewCLI()
	if err := cli.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
