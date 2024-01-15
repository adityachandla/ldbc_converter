package mapping_stage

import (
	"io"
	"os"

	"github.com/go-yaml/yaml"
)

type mappingConfig struct {
	InDir        string        `yaml:"inDir"`
	OutDir       string        `yaml:"outDir"`
	NodeMappings []nodeMapping `yaml:"nodeMappings"`
}

type nodeMapping struct {
	// The file that contains all values for a particular
	// node type. Ex person.csv for person.
	MapInputFile string `yaml:"inputFile"`
	// The name of the field that contains the values
	MappingField string `yaml:"mappingField"`
	// Dependencies where that id is used.
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
