package main

import (
	"github.com/spf13/cobra"
	"log"
)

var RootCmd = cobra.Command{
	Use: "stlf",
}

func main() {
	err := RootCmd.Execute()
	if err != nil {
		log.Fatalf("%++v", err)
	}
}
