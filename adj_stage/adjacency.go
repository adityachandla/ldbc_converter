package adj_stage

import (
	"io"
	"os"
)

const FILE_FORMAT string = "s_%d_e_%d.csv"
const inDir = "./mapping/stage_7/"

// We separate out the logic for edge reading and writing to file.
//
// EdgeProducer reads edges one by one while also mapping string
// labels to integers.
//
// Parititioner creates partitions for every edge and adds every
// edge to its correct partition.
func RunAdjacencyStage(partitionSizeMb int, outDir string) {
	os.Mkdir(outDir, os.ModePerm)
	partitioner := createPartitioner(outDir, partitionSizeMb)
	edgeProducer := createEdgeProducer()
	edges, err := edgeProducer.getEdges()
	for err != io.EOF {
		for _, e := range edges {
			partitioner.processEdge(e)
		}
		edges, err = edgeProducer.getEdges()
	}
}
