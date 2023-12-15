package main

import (
	"fmt"
	"io"
	"os"

	"github.com/adityachandla/ldbc_converter/csv_util"
	"github.com/go-yaml/yaml"
)

type mappingConfig struct {
	InDir        string        `yaml:"inDir"`
	OutDir       string        `yaml:"outDir"`
	NodeMappings []nodeMapping `yaml:"nodeMappings"`
}

type nodeMapping struct {
	MapInputFile string                `yaml:"inputFile"`
	MappingField string                `yaml:"mappingField"`
	Dependencies []mappingDependencies `yaml:"dependencies"`
}

type mappingDependencies struct {
	File   string   `yaml:"file"`
	Fields []string `yaml:"fields"`
}

func readMappingConfig(file string) *mappingConfig {
	f, err := os.Open(file)
	if err != nil {
		panic(err)
	}
	configBytes, err := io.ReadAll(f)
	if err != nil {
		panic(err)
	}
	var config mappingConfig
	yaml.Unmarshal(configBytes, &config)
	return &config
}

func RunMappingStage(configFile string) {
	config := readMappingConfig(configFile)
	os.Mkdir(config.OutDir, os.ModePerm)
	//TODO uncomment and finish
	//This will change after every mapping
	//prevDir := config.InDir
	//for idx, node := range config.NodeMappings {
	//dirName := config.OutDir + fmt.Sprintf("stage_%d/", idx)
	//os.Mkdir(dirName, os.ModePerm)
	//We read from prevDir and into dirName
	//We also need to create a hard link to the files
	//prevDir = dirName
	//}
}

func mapPerson(personFile string) {
	newNodeId = 1
	mapping := createMapping(personFile)
	fmt.Println(len(mapping))
}

func createMapping(personFile string) map[string]uint32 {
	csvReader := csv_util.CreateCsvFileReader(personFile)
	headers := csvReader.GetHeaders()
	idIndex := -1
	for i := range headers {
		if headers[i] == "id" {
			idIndex = i
			break
		}
	}
	if idIndex == -1 {
		panic("Id not found in header")
	}
	mapping := make(map[string]uint32)
	row, err := csvReader.ReadRow()
	for err == nil {
		mapping[row[idIndex]] = newNodeId
		newNodeId++
		row, err = csvReader.ReadRow()
	}
	return mapping
}
