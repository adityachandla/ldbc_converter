package main

import "fmt"

var newNodeId uint32

func main() {
	fmt.Println("Running Strip stage")
	RunStripStage("stripStage.yaml")
	fmt.Println("Running mapping stage")
	RunMappingStage("mappingStage.yaml")
}
