package adj_stage

import (
	"bufio"
	"fmt"
	"os"
)

// This format is src, label, destination, incoming
const EDGE_FORMAT = "(%d,%d,%d,%v)\n"

type Partitioner []Partition

func createPartitioner(numPartitions, nodeCount uint32, outDir string) Partitioner {
	partSize := (nodeCount + numPartitions - 1) / numPartitions
	partitioner := make([]Partition, 0, numPartitions)
	start := uint32(0)
	for i := uint32(0); i < numPartitions; i++ {
		end := start + partSize
		partitioner = append(partitioner, createPartition(start, end, outDir))
		start += partSize
	}
	return partitioner
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
