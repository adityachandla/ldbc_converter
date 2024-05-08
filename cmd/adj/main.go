package main

import (
	"flag"
	"fmt"

	"github.com/adityachandla/ldbc_converter/adj_stage"
)

var (
	partitionSizeMb = flag.Int("partSize", 16, "Size of partition in megabytes")
	outDir          = flag.String("outDir", "", "Output directory for the adjacency")
)

func main() {
	flag.Parse()
	if *outDir == "" {
		fmt.Println("Enter output directory using -outDir flag")
		return
	}
	adj_stage.RunAdjacencyStage(*partitionSizeMb, *outDir)
	fmt.Printf("Finished creating partitions of %d Mb", *partitionSizeMb)
}
