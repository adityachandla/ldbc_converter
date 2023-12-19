package adj_stage

import (
	"bufio"
	"fmt"
	"os"
)

type Partitioner struct {
	parts []Partition
}

func createPartitioner(numPartitions, nodeCount uint32, outDir string) *Partitioner {
	partSize := (nodeCount + numPartitions - 1) / numPartitions
	partitioner := Partitioner{
		parts: make([]Partition, 0, numPartitions),
	}
	start := uint32(0)
	for i := uint32(0); i < numPartitions; i++ {
		end := start + partSize
		partitioner.parts = append(partitioner.parts, createPartition(start, end, outDir))
		start += partSize
	}
	return &partitioner
}

func (p *Partitioner) processEdge(e Edge) {
	low := 0
	high := len(p.parts) - 1
	for low <= high {
		mid := (low + high) / 2
		if p.parts[mid].contains(e) {
			p.parts[mid].process(e)
			break
		} else if p.parts[mid].start > e.src {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}
	if high < low {
		panic(fmt.Errorf("Unable to process Edge:%v", e))
	}
}

func (p *Partitioner) Close() {
	for i := range p.parts {
		p.parts[i].Close()
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
	filePath := fmt.Sprintf("%ss_%d_e_%d.csv", outDir, start, end)
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
	toWrite := fmt.Sprintf("(%d,%d,%d)\n", e.src, e.label, e.dest)
	p.writer.WriteString(toWrite)
}
