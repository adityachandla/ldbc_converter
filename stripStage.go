package main

import (
	"io"
	"os"
	"sync"

	"github.com/adityachandla/ldbc_converter/csv_util"
	"github.com/adityachandla/ldbc_converter/file_util"
	"github.com/go-yaml/yaml"
)

type StripStage struct {
	OutDir         string              `yaml:"outDir"`
	InputDirPrefix string              `yaml:"inputDirPrefix"`
	Mappings       []StripStageMapping `yaml:"mappings"`
}

type StripStageMapping struct {
	Dir     string   `yaml:"dir"`
	OutFile string   `yaml:"outFile"`
	Headers []string `yaml:"headers"`
}

func readStripConfig(configName string) *StripStage {
	f, err := os.Open(configName)
	if err != nil {
		panic(err)
	}
	resBytes, err := io.ReadAll(f)
	if err != nil {
		panic(err)
	}
	var stripStage StripStage
	yaml.Unmarshal(resBytes, &stripStage)
	return &stripStage
}

func RunStripStage(configName string) {
	config := readStripConfig(configName)
	os.Mkdir(config.OutDir, os.ModePerm)
	wg := sync.WaitGroup{}
	for i := range config.Mappings {
		inDir := config.InputDirPrefix + config.Mappings[i].Dir
		outFile := config.OutDir + config.Mappings[i].OutFile
		headers := config.Mappings[i].Headers
		wg.Add(1)
		go func() {
			defer wg.Done()
			stripCsv(headers, inDir, outFile)
		}()
	}
	wg.Wait()
}

// Combines input files in all directories and outputs a
// single file that only contains the headers that are passed
// into the function.
func stripCsv(header []string, inDir, outFile string) {
	resultWriter := csv_util.CreateCsvFileWriter(outFile)
	defer resultWriter.Close()
	resultWriter.WriteRow(header)
	files, err := file_util.GetCsvFiles(inDir)
	if err != nil {
		panic(err)
	}
	for _, f := range files {
		inputFilePath := inDir + f
		reader := csv_util.CreateCsvFileReader(inputFilePath)
		res, err := reader.ReadRowValues(header)
		for err == nil {
			resultWriter.WriteRow(res)
			res, err = reader.ReadRowValues(header)
		}
		reader.Close()
	}
}
