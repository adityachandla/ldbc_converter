package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCsrCreation(t *testing.T) {
	csr := createCsr("testfile.csv")
	if len(csr.edges) != 7 {
		t.Errorf("Expected 7 edges got %d", len(csr.edges))
	}
	assert.Equal(t, len(csr.edges), 7)
	expectedEdges := []edge{{1, 5}, {1, 7}, {2, 3}, {2, 4}, {2, 2}, {2, 5}, {3, 1}}
	assert.Equal(t, csr.edges, expectedEdges)
	assert.Equal(t, csr.nodeIndex, []uint32{0, 4})
}
