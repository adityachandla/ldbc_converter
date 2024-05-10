package main

import (
	"flag"
	"fmt"
	"strings"

	"github.com/adityachandla/ldbc_converter/adj_stage"
)

var (
	partitionSizeMb = flag.Int("partSize", 16, "Size of partition in megabytes")
	outDir          = flag.String("outDir", "", "Output directory for the adjacency")
	inDir           = flag.String("inDir", "./mapping/stage_7/", "Directory with results of mapping stage")
)

func main() {
	flag.Parse()
	if *outDir == "" {
		fmt.Println("Enter output directory using -outDir flag")
		return
	}
	if !strings.HasSuffix(*outDir, "/") {
		*outDir += "/"
	}
	adj_stage.RunAdjacencyStage(*partitionSizeMb, *inDir, *outDir)
	fmt.Printf("Finished creating partitions of %d Mb", *partitionSizeMb)
}
