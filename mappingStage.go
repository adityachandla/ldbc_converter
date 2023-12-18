package main

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/adityachandla/ldbc_converter/csv_util"
	"github.com/adityachandla/ldbc_converter/file_util"
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

func (dep *mappingDependencies) processDependency(mapping map[string]uint32,
	newDir, oldDir string) {
	oldFileReader := csv_util.CreateCsvFileReader(oldDir + dep.File)
	newFileWriter := csv_util.CreateCsvFileWriter(newDir + dep.File)
	newFileWriter.WriteRow(oldFileReader.GetHeaders())
	indicesToUpdate := oldFileReader.GetHeaderIndices(dep.Fields)
	row, err := oldFileReader.ReadRow()
	for err == nil {
		for _, i := range indicesToUpdate {
			if newVal, ok := mapping[row[i]]; ok {
				row[i] = fmt.Sprintf("%d", newVal)
			} else {
				panic(row[i] + " Not found in mapping")
			}
		}
		newFileWriter.WriteRow(row)
		row, err = oldFileReader.ReadRow()
	}
	oldFileReader.Close()
	newFileWriter.Close()
}

var nodeId uint32 = 0

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
	//This will change after every mapping
	prevDir := config.InDir
	for idx, node := range config.NodeMappings {
		dirName := config.OutDir + fmt.Sprintf("stage_%d/", idx)
		os.Mkdir(dirName, os.ModePerm)
		runMappingForNode(&node, dirName, prevDir)
		copyUnmodifiedFiles(&node, dirName, prevDir)
		fmt.Printf("Finished writing to %s\n", dirName)
		prevDir = dirName
	}
}

func copyUnmodifiedFiles(node *nodeMapping, newDir, oldDir string) {
	allFiles, err := file_util.GetFilesInDir(oldDir)
	if err != nil {
		panic(fmt.Errorf("Unable to open directory %s\n%s", oldDir, err))
	}
	modified := make(map[string]struct{})
	for _, dep := range node.Dependencies {
		modified[dep.File] = struct{}{}
	}
	for _, f := range allFiles {
		if _, ok := modified[f]; !ok {
			//This file was unmodified create a symlink
			os.Link(oldDir+f, newDir+f)
		}
	}
}

// Apply the nodeMapping to the nodes and update all the files in
// old dir and add the updated files to new dir. Create symlinks
// for files that remain unchanged.
func runMappingForNode(node *nodeMapping, newDir, oldDir string) {
	inputFileFullPath := oldDir + node.MapInputFile
	mapping := createMapping(inputFileFullPath, node.MappingField)
	var wg sync.WaitGroup
	for _, dep := range node.Dependencies {
		wg.Add(1)
		d := dep
		go func() {
			defer wg.Done()
			d.processDependency(mapping, newDir, oldDir)
		}()
	}
	wg.Wait()
}

func createMapping(fileName, fieldName string) map[string]uint32 {
	csvReader := csv_util.CreateCsvFileReader(fileName)
	headers := csvReader.GetHeaders()
	idIndex := -1
	for i := range headers {
		if headers[i] == fieldName {
			idIndex = i
			break
		}
	}
	if idIndex == -1 {
		panic(fieldName + " not found in header")
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
