package main

import (
	"fmt"

	"github.com/adityachandla/ldbc_converter/adj_stage"
)

func main() {
	//fmt.Println("Running Strip stage")
	//strip_stage.RunStripStage("strip_stage/stripStage.yaml")

	//fmt.Println("Running mapping stage")
	//numNodes := mapping_stage.RunMappingStage("mapping_stage/mappingStage.yaml")

	numNodes := uint32(2997352)
	fmt.Printf("Running adjacency stage. NumNodes=%d\n", numNodes)
	adj_stage.RunAdjacencyStage(numNodes, "adj_stage/adjStage.yaml")
}
