package adj_stage

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/adityachandla/ldbc_converter/file_util"
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

func splitFiles(baseDir string, sizeMb int) {
	filesToSplit, err := file_util.GetFilesLargerThan(baseDir, sizeMb)
	if err != nil {
		panic(err)
	}
	for len(filesToSplit) > 0 {
		//Split the file into two
		fmt.Printf("Splitting %d files into two\n", len(filesToSplit))
		var wg sync.WaitGroup
		for _, f := range filesToSplit {
			wg.Add(1)
			fileName := f
			go func() {
				defer wg.Done()
				splitFile(baseDir, fileName)
			}()
		}
		wg.Wait()
		filesToSplit, err = file_util.GetFilesLargerThan(baseDir, sizeMb)
		if err != nil {
			panic(err)
		}
	}
}

func splitFile(dir, fileName string) {
	var start, end uint32
	fmt.Sscanf(fileName, FILE_FORMAT, &start, &end)
	mid := (start + end) / 2
	low, err := os.Create(fmt.Sprintf(dir+FILE_FORMAT, start, mid))
	if err != nil {
		panic("Unable to create file")
	}
	defer low.Close()
	high, err := os.Create(fmt.Sprintf(dir+FILE_FORMAT, mid, end))
	if err != nil {
		panic("Unable to create file")
	}
	defer high.Close()
	old, err := os.Open(dir + fileName)
	if err != nil {
		panic("Unable to open old file")
	}
	defer old.Close()
	oldReader := bufio.NewReader(old)
	lowWriter := bufio.NewWriter(low)
	highWriter := bufio.NewWriter(high)

	line, err := oldReader.ReadString('\n')
	for err == nil {
		var src, label, dest uint32
		fmt.Sscanf(line, "(%d,%d,%d)\n", &src, &label, &dest)
		line, err = oldReader.ReadString('\n')
		//Higher value is not inclusive.
		if src >= mid {
			highWriter.WriteString(line)
		} else {
			lowWriter.WriteString(line)
		}
	}

	err = os.Remove(dir + fileName)
	if err != nil {
		panic(fmt.Errorf("Unable to remove old file\n%s", err))
	}
}

func RunAdjacencyStage(nodeCount uint32, configFile string) string {
	config := readConfig(configFile)
	os.Mkdir(config.OutDir, os.ModePerm)
	partitioner := createPartitioner(config.Partitions, nodeCount, config.OutDir)
	edgeProducer := createEdgeProducer(config.InDir)
	edges, err := edgeProducer.getEdges()
	for err == nil {
		for _, e := range edges {
			partitioner.processEdge(e)
		}
		edges, err = edgeProducer.getEdges()
	}
	//Files larger than a given size should be split.
	splitFiles(config.OutDir, config.FileSizeMb)
	return config.OutDir
}
