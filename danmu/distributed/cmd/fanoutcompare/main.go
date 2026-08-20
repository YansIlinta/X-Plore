package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/YansIlinta/danmu-distributed/fanoutevidence"
)

func main() {
	leftPath := flag.String("left", "", "left fanout evidence artifact")
	rightPath := flag.String("right", "", "right fanout evidence artifact")
	output := flag.String("output", "", "optional JSON comparison output path")
	flag.Parse()
	if *leftPath == "" || *rightPath == "" {
		fatal(fmt.Errorf("both -left and -right are required"))
	}

	left, err := readArtifact(*leftPath)
	if err != nil {
		fatal(fmt.Errorf("left artifact: %w", err))
	}
	right, err := readArtifact(*rightPath)
	if err != nil {
		fatal(fmt.Errorf("right artifact: %w", err))
	}
	cmp, err := fanoutevidence.Compare(left, right)
	if err != nil {
		fatal(err)
	}

	data, err := json.MarshalIndent(cmp, "", "  ")
	if err != nil {
		fatal(err)
	}
	data = append(data, '\n')
	if *output != "" {
		if err := os.WriteFile(*output, data, 0o644); err != nil {
			fatal(err)
		}
	}
	_, _ = os.Stdout.Write(data)
	if !cmp.Comparable {
		os.Exit(2)
	}
}

func readArtifact(path string) (fanoutevidence.Artifact, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return fanoutevidence.Artifact{}, err
	}
	var a fanoutevidence.Artifact
	if err := json.Unmarshal(data, &a); err != nil {
		return fanoutevidence.Artifact{}, err
	}
	if a.SchemaVersion <= 0 {
		return fanoutevidence.Artifact{}, fmt.Errorf("missing/invalid schema_version")
	}
	return a, nil
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "fanoutcompare:", err)
	os.Exit(1)
}
