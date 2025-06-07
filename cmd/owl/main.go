package main

import (
	"os"

	"github.com/owlmessenger/owl/src/owlcmd"
)

func main() {
	if err := owlcmd.NewRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}
