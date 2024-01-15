package mapping_stage

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/adityachandla/ldbc_converter/csv_util"
	"github.com/adityachandla/ldbc_converter/file_util"
)

// This needs to be a global variable because
// we want to have the same increasing sequence
// across different files.
var nodeId uint32 = 0

func RunMappingStage(configFile string) (uint32, string) {
	config := readMappingConfig(configFile)
	os.Mkdir(config.OutDir, os.ModePerm)
	// This will change after every mapping
	prevDir := config.InDir

	// This map needs to be preserved so it is in the root dir.
	nodeRangeWriter := csv_util.CreateCsvFileWriter("nodeMap.csv")
	defer nodeRangeWriter.Close()
	nodeRangeWriter.WriteRow([]string{"type", "start", "end"})

	for idx, node := range config.NodeMappings {
		dirName := config.OutDir + fmt.Sprintf("stage_%d/", idx)
		os.Mkdir(dirName, os.ModePerm)
		start := fmt.Sprintf("%d", nodeId)

		runMappingForNode(&node, dirName, prevDir)
		copyUnmodifiedFiles(&node, dirName, prevDir)
		nodeName := strings.TrimSuffix(node.MapInputFile, ".csv")

		//Write start and end range to a file
		end := fmt.Sprintf("%d", nodeId-1)
		nodeRangeWriter.WriteRow([]string{nodeName, start, end})
		fmt.Printf("Completed. File=%s\n", node.MapInputFile)

		prevDir = dirName
	}
	return nodeId, config.OutDir
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
			processDependency(d, mapping, newDir, oldDir)
		}()
	}
	wg.Wait()
}

// This changes the old id to new id in all the dependent files.
func processDependency(dep mappingDependencies, mapping map[uint64]uint32, newDir, oldDir string) {
	oldFileReader := csv_util.CreateCsvFileReader(oldDir + dep.File)
	newFileWriter := csv_util.CreateCsvFileWriter(newDir + dep.File)
	newFileWriter.WriteRow(oldFileReader.GetHeaders())
	indicesToUpdate := oldFileReader.GetHeaderIndices(dep.Fields)
	row, err := oldFileReader.ReadRow()
	for err == nil {
		for _, i := range indicesToUpdate {
			if row[i] == "" {
				continue
			}
			id64 := toUInt64(row[i])
			if newVal, ok := mapping[id64]; ok {
				row[i] = fmt.Sprintf("%d", newVal)
			} else {
				err := fmt.Errorf("%s not found. Row=%v. dep=%v", row[i], row, dep)
				panic(err)
			}
		}
		newFileWriter.WriteRow(row)
		row, err = oldFileReader.ReadRow()
	}
	oldFileReader.Close()
	newFileWriter.Close()
}

// The mapping should be created from a file where there is no repetition
// and all possible values of a node are contained in it.
func createMapping(fileName, fieldName string) map[uint64]uint32 {
	csvReader := csv_util.CreateCsvFileReader(fileName)
	headers := csvReader.GetHeaders()
	idIndex := slices.Index(headers, fieldName)
	if idIndex == -1 {
		panic(fieldName + " not found in header")
	}
	mapping := make(map[uint64]uint32)
	row, err := csvReader.ReadRow()
	for err == nil {
		idUint := toUInt64(row[idIndex])
		mapping[idUint] = nodeId
		nodeId++
		row, err = csvReader.ReadRow()
	}
	return mapping
}

// Create a symlink for all the files that were not modified in the
// previous run.
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

func toUInt64(s string) uint64 {
	id64, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		panic(err)
	}
	return id64
}
