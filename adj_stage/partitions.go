package adj_stage

import (
	"bufio"
	"fmt"
	"os"

	"github.com/adityachandla/ldbc_converter/csv_util"
	"github.com/adityachandla/ldbc_converter/file_util"
)

// EDGE_FORMAT This format is src, label, destination, outgoing
const EDGE_FORMAT = "(%d,%d,%d,%v)\n"

// OUTGOING OFFSET and INCOMING OFFSET as uint32s
const SRC_SIZE = 4 + 4

// DESTINATION and LABEL as uint32s
const EDGE_SIZE = 4 + 4

type Partitioner []Partition

func createPartitioner(outDir string, partitionSizeMb int) Partitioner {
	nodeCountMap := getNodeCountMap(inDir)
	nodeCount := uint32(len(nodeCountMap))
	targetSizeBytes := partitionSizeMb * 1024 * 1024
	start := uint32(0)
	currSize := 0
	partitioner := make([]Partition, 0, 32)

	for i := uint32(0); i <= nodeCount; i++ {
		currSize += SRC_SIZE + (EDGE_SIZE * nodeCountMap[i])
		if currSize >= targetSizeBytes {
			partition := createPartition(start, i+1, outDir)
			partitioner = append(partitioner, partition)

			currSize = 0
			start = i + 1
		}
	}
	if currSize > 0 {
		partition := createPartition(start, nodeCount, outDir)
		partitioner = append(partitioner, partition)
	}

	return partitioner
}

func getNodeCountMap(dir string) map[uint32]int {
	files, err := file_util.GetCsvFiles(dir)
	if err != nil {
		panic(err)
	}
	res := make(map[uint32]int)
	for _, f := range files {
		r := csv_util.CreateCsvFileReader(dir + f)
		row, err := r.ReadRow()
		for err == nil {
			//Count outgoing edge for src
			src := toUint32(row[0])

			//Count incoming edges for dest
			for _, destStr := range row[1:] {
				if destStr == "" {
					continue
				}
				dest := toUint32(destStr)
				res[dest]++
				res[src]++
			}

			row, err = r.ReadRow()
		}
	}
	return res
}

// Find the right partition and delegate the processing
// to that partition.
func (p Partitioner) processEdge(e Edge) {
	low := 0
	high := len(p) - 1
	for low <= high {
		mid := (low + high) / 2
		if p[mid].contains(e) {
			p[mid].process(e)
			return
		} else if p[mid].start > e.src {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}
	// This may happen when the ids of the last node and
	// end of the last partition align.
	panic(fmt.Errorf("Unable to process Edge:%v", e))
}

func (p Partitioner) Close() {
	for i := range p {
		p[i].Close()
	}
}

// End is exclusive for all but the last
// partition.
type Partition struct {
	file   *os.File
	writer *bufio.Writer
	start  uint32
	end    uint32
}

func createPartition(start, end uint32, outDir string) Partition {
	filePath := fmt.Sprintf("%s"+FILE_FORMAT, outDir, start, end)
	f, err := os.Create(filePath)
	if err != nil {
		panic("Unable to create required file")
	}
	writer := bufio.NewWriter(f)
	return Partition{
		file:   f,
		writer: writer,
		start:  start,
		end:    end,
	}
}

func (p *Partition) Close() {
	p.writer.Flush()
	p.file.Close()
}

func (p *Partition) contains(e Edge) bool {
	return p.start <= e.src && p.end > e.src
}

func (p *Partition) process(e Edge) {
	toWrite := fmt.Sprintf(EDGE_FORMAT, e.src, e.label, e.dest, e.outgoing)
	p.writer.WriteString(toWrite)
}
