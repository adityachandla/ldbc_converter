package main

import (
	"fmt"

	"github.com/adityachandla/ldbc_converter/adj_stage"
	"github.com/adityachandla/ldbc_converter/file_util"
	"github.com/adityachandla/ldbc_converter/mapping_stage"
	"github.com/adityachandla/ldbc_converter/strip_stage"
)

func main() {
	fmt.Println("Running Strip stage")
	stripDir := strip_stage.RunStripStage("strip_stage/stripStage.yaml")

	fmt.Println("Running mapping stage")
	numNodes, mapDir := mapping_stage.RunMappingStage("mapping_stage/mappingStage.yaml")
	file_util.RemoveDir(stripDir)

	fmt.Printf("Running adjacency stage. NumNodes=%d\n", numNodes)
	adjDir := adj_stage.RunAdjacencyStage(numNodes, "adj_stage/adjStage.yaml")
	file_util.RemoveDir(mapDir)
	fmt.Println(adjDir)
}
