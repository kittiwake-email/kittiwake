package main

import (
	"flag"
	"os"
)

func newFlagSet(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	return fs
}

// configFlag registers --config and its short form -c on the flag set.
func configFlag(fs *flag.FlagSet) *string {
	var path string
	const usage = "path to the YAML config (default $DOVETAIL_CONFIG or /etc/dovetail/dovetail.yaml)"
	fs.StringVar(&path, "config", "", usage)
	fs.StringVar(&path, "c", "", "shorthand for --config")
	return &path
}
