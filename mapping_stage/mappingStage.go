package mapping_stage

import (
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/adityachandla/ldbc_converter/csv_util"
	"github.com/adityachandla/ldbc_converter/file_util"
)

func (dep *mappingDependencies) processDependency(mapping map[uint64]uint32,
	newDir, oldDir string) {
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

var nodeId uint32 = 0

func RunMappingStage(configFile string) (uint32, string) {
	config := readMappingConfig(configFile)
	os.Mkdir(config.OutDir, os.ModePerm)
	//This will change after every mapping
	prevDir := config.InDir
	for idx, node := range config.NodeMappings {
		dirName := config.OutDir + fmt.Sprintf("stage_%d/", idx)
		os.Mkdir(dirName, os.ModePerm)
		runMappingForNode(&node, dirName, prevDir)
		copyUnmodifiedFiles(&node, dirName, prevDir)
		fmt.Printf("Completed. File=%s CurrNodeId=%d\n", node.MapInputFile, nodeId)
		prevDir = dirName
	}
	return nodeId, config.OutDir
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

// The mapping should be created from a file where there is no repetition
// and all possible values of a node are contained in it.
func createMapping(fileName, fieldName string) map[uint64]uint32 {
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

func toUInt64(s string) uint64 {
	id64, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		panic(err)
	}
	return id64
}
