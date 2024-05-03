package adj_stage

import (
	"io"
	"os"

	"github.com/go-yaml/yaml"
)

const FILE_FORMAT string = "s_%d_e_%d.csv"

type adjacencyConfig struct {
	InDir      string `yaml:"inputDir"`
	Partitions uint32 `yaml:"numPartitions"`
	OutDir     string `yaml:"outputDir"`
	FileSizeMb int    `yaml:"fileSizeMb"`
}

func readConfig(file string) *adjacencyConfig {
	f, err := os.Open(file)
	if err != nil {
		panic("Unable to open config file")
	}
	inBytes, err := io.ReadAll(f)
	if err != nil {
		panic("Unable to read config file")
	}
	var config adjacencyConfig
	yaml.Unmarshal(inBytes, &config)
	return &config
}

// We separate out the logic for edge reading and writing to file.
//
// EdgeProducer reads edges one by one while also mapping string
// labels to integers.
//
// Parititioner creates partitions for every edge and adds every
// edge to its correct partition.
func RunAdjacencyStage(nodeCount uint32, configFile string) string {
	config := readConfig(configFile)
	os.Mkdir(config.OutDir, os.ModePerm)
	partitioner := createPartitioner(config, nodeCount)
	edgeProducer := createEdgeProducer(config.InDir)
	edges, err := edgeProducer.getEdges()
	for err != io.EOF {
		for _, e := range edges {
			partitioner.processEdge(e)
		}
		edges, err = edgeProducer.getEdges()
	}
	//Files larger than a given size should be split.
	//splitFiles(config.OutDir, config.FileSizeMb)
	return config.OutDir
}
