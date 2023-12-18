package strip_stage

import (
	"io"
	"os"

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
