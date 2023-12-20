package adj_stage

import (
	"fmt"
	"io"
	"strconv"

	"github.com/adityachandla/ldbc_converter/csv_util"
	"github.com/adityachandla/ldbc_converter/file_util"
)

type Edge struct {
	src, dest, label uint32
}

type EdgeProducer struct {
	filePaths  []string
	currIdx    int //Current index of the filePath
	reader     *csv_util.CsvFileReader
	edgeLabels []uint32 //Edge labels for the current file
	labelId    uint32   //Start: 1 Stores: 1+the highest label assigned
}

func createEdgeProducer(inDir string) *EdgeProducer {
	names, err := file_util.GetCsvFiles(inDir)
	if err != nil {
		panic(err)
	}
	for i := range names {
		names[i] = inDir + names[i]
	}
	reader := csv_util.CreateCsvFileReader(names[0])
	fmt.Printf("Reading file %s\n", names[0])
	ep := &EdgeProducer{
		filePaths: names,
		currIdx:   0,
		reader:    reader,
		labelId:   1,
	}
	ep.assignLabelsForCurrentFile()
	return ep
}

func (ep *EdgeProducer) assignLabelsForCurrentFile() {
	headers := ep.reader.GetHeaders()
	//First one is -1 the rest are labels
	ep.edgeLabels = make([]uint32, len(headers))
	ep.edgeLabels[0] = 0
	for i := 1; i < len(ep.edgeLabels); i++ {
		ep.edgeLabels[i] = ep.labelId
		fmt.Printf("Assigned label %d to header %s\n", ep.labelId, headers[i])
		ep.labelId++
	}
}

func (ep *EdgeProducer) getEdges() ([]Edge, error) {
	row, err := ep.reader.ReadRow()
	for err == io.EOF {
		//return EOF if this was the last file
		if ep.currIdx == len(ep.filePaths)-1 {
			return nil, err
		}
		//Open the next file for reading.
		ep.reader.Close()
		ep.currIdx++
		ep.reader = csv_util.CreateCsvFileReader(ep.filePaths[ep.currIdx])
		fmt.Printf("Reading file %s\n", ep.filePaths[ep.currIdx])
		ep.assignLabelsForCurrentFile()
		row, err = ep.reader.ReadRow()
	}
	//At this point, row contains actual values.
	edges := make([]Edge, len(row)-1)
	src, err := strconv.ParseUint(row[0], 10, 32)
	if err != nil {
		panic(err)
	}
	src32 := uint32(src)
	for i := 1; i < len(row); i++ {
		dest, err := strconv.ParseUint(row[i], 10, 32)
		if err != nil {
			//It is possible that the row was empty.
			if row[i] == "" {
				continue
			} else {
				panic(err)
			}
		}
		dest32 := uint32(dest)
		edges[i-1] = Edge{
			src:   src32,
			dest:  dest32,
			label: ep.edgeLabels[i],
		}
	}
	return edges, nil
}
