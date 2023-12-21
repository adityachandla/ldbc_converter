package main

import (
	"os"
	"testing"

	"github.com/adityachandla/ldbc_converter/bin_util"
	"github.com/stretchr/testify/assert"
)

func TestCsrCreation(t *testing.T) {
	csr := createCsr("testfile.csv")
	assert.Equal(t, len(csr.edges), 7)
	expectedEdges := []edge{{1, 5}, {1, 7}, {2, 3}, {2, 4}, {2, 2}, {2, 5}, {3, 1}}
	assert.Equal(t, csr.edges, expectedEdges)
	assert.Equal(t, csr.nodeIndex, []uint32{0, 4})
}

func TestWriteToFile(t *testing.T) {
	csr := createCsr("testfile.csv")
	csr.writeToFile("test.csr")
	reader, err := bin_util.CreateReader("test.csr")
	defer os.Remove("test.csr")
	assert.NoError(t, err)
	//start and end
	assert.Equal(t, uint32(10), read(t, reader))
	assert.Equal(t, uint32(11), read(t, reader))

	//node index
	assert.Equal(t, uint32(0), read(t, reader))
	assert.Equal(t, uint32(4), read(t, reader))

	//First edge
	assert.Equal(t, uint32(1), read(t, reader))
	assert.Equal(t, uint32(5), read(t, reader))
}

func read(t *testing.T, reader bin_util.BinaryReader) uint32 {
	v, err := reader.ReadUint32()
	assert.NoError(t, err)
	return v
}
