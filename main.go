package main

import (
	"fmt"

	"github.com/adityachandla/ldbc_converter/mapping_stage"
	"github.com/adityachandla/ldbc_converter/strip_stage"
)

func main() {
	fmt.Println("Running Strip stage")
	strip_stage.RunStripStage("strip_stage/stripStage.yaml")

	fmt.Println("Running mapping stage")
	numNodes := mapping_stage.RunMappingStage("mapping_stage/mappingStage.yaml")

	fmt.Printf("Running adjacency stage. NumNodes=%d\n", numNodes)
}
