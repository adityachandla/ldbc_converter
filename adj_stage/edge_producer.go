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
	outgoing         bool
}

type EdgeProducer struct {
	filePaths  []string
	currIdx    int //Current index of the filePath
	reader     *csv_util.CsvFileReader
	edgeLabels []uint32 //Edge labels for the current file
	labelId    uint32   //Starting val: 1 Curr Val: 1+the highest label assigned
}

func createEdgeProducer(inDir string) *EdgeProducer {
	// Gets the names in sorted format so the order is the
	// same every time.
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

// Assigns edge labels to the new file.
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

// Gets the edges produced after reading the next
// line among all the files.
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
	edges := make([]Edge, 0, len(row)-1)
	src := toUint32(row[0])
	for i := 1; i < len(row); i++ {
		if row[i] == "" {
			continue
		}
		dest := toUint32(row[i])
		toAdd := getIncomingAndOutgoing(src, dest, ep.edgeLabels[i])
		edges = append(edges, toAdd[0], toAdd[1])
	}
	return edges, nil
}

func toUint32(input string) uint32 {
	res, err := strconv.ParseUint(input, 10, 32)
	if err != nil {
		panic(err)
	}
	return uint32(res)
}

func getIncomingAndOutgoing(src, dest, label uint32) [2]Edge {
	var res [2]Edge
	//Outgoing edge
	res[0] = Edge{
		src:      src,
		dest:     dest,
		label:    label,
		outgoing: true,
	}
	//Incoming edge
	res[1] = Edge{
		src:      dest,
		dest:     src,
		label:    label,
		outgoing: false,
	}
	return res
}
