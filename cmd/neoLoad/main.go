package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/adityachandla/ldbc_converter/adj_stage"
	"github.com/adityachandla/ldbc_converter/csv_util"
	"github.com/adityachandla/ldbc_converter/file_util"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

const DB_NAME = "neo4j1"
const username = "neo4j"
const password = "hello123"
const uri = "neo4j://localhost"
const query = "MERGE (n1:NODE {uid: $id1}) MERGE (n2:NODE {uid: $id2}) MERGE (n1)-[:%s]->(n2)"

var driver neo4j.DriverWithContext

var offset = 0

// Mapping from a label to the query corresponding to that label.
var queryMap map[uint32]string

type edgeReader struct {
	filePaths []string
	currIdx   int
	f         *os.File
	reader    *bufio.Reader
}

func NewEdgeReader(inDir string) *edgeReader {
	files, err := file_util.GetFilesInDir(inDir)
	if err != nil {
		panic(err)
	}
	er := edgeReader{filePaths: make([]string, len(files))}
	for i := range files {
		er.filePaths[i] = inDir + files[i]
	}
	er.currIdx = 0
	er.f, err = os.Open(er.filePaths[0])
	if err != nil {
		panic(err)
	}
	er.reader = bufio.NewReader(er.f)
	return &er
}

func (er *edgeReader) ReadRelation() (edge, error) {
	line, err := er.getline()
	for err == nil {
		if strings.HasSuffix(line, "ue)") {
			return parseEdge(line), nil
		}
		line, err = er.getline()
	}
	return edge{}, io.EOF
}

func parseEdge(line string) edge {
	var src, label, dest uint32
	var outgoing bool
	fmt.Sscanf(line, adj_stage.EDGE_FORMAT, &src, &label, &dest, &outgoing)
	return edge{src, label, dest}
}

func (er *edgeReader) getline() (string, error) {
	line, err := er.reader.ReadString('\n')
	if err == io.EOF {
		er.f.Close()

		er.currIdx += 1
		if er.currIdx == len(er.filePaths) {
			return "", io.EOF
		}
		er.f, err = os.Open(er.filePaths[er.currIdx])
		if err != nil {
			panic(err)
		}
		er.reader = bufio.NewReader(er.f)
		return er.getline()
	} else if err != nil {
		panic(err)
	}
	line = strings.Trim(line, " \n")
	return line, nil
}

type edge struct {
	src, label, dest uint32
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Enter input directory.")
	}
	inDir := os.Args[1]
	if !strings.HasSuffix(inDir, "/") {
		inDir += "/"
	}
	if len(os.Args) == 3 {
		var err error
		offset, err = strconv.Atoi(os.Args[2])
		if err != nil {
			panic(err)
		}
	}
	createDriver()
	createIndex()
	createQueryMap()
	er := NewEdgeReader(inDir)
	defer driver.Close(context.Background())

	bufSize := 50_000
	edgeBuffer := make([]edge, bufSize)
	bufIdx := 0
	e, err := er.ReadRelation()
	total := 0
	for err == nil {
		edgeBuffer[bufIdx] = e
		bufIdx++
		if bufIdx == bufSize {
			if total >= offset {
				addData(edgeBuffer)
			}
			total += bufSize
			fmt.Printf("Added %d Rows\r", total)
			bufIdx = 0
		}
		e, err = er.ReadRelation()
	}
	addData(edgeBuffer[:bufIdx])
}

func createQueryMap() {
	queryMap = make(map[uint32]string, 23)
	reader := csv_util.CreateCsvFileReader("./cmd/neoLoad/edgeMap.csv")
	defer reader.Close()
	row, err := reader.ReadRow()
	for err == nil {
		q := fmt.Sprintf(query, row[0])
		label, _ := strconv.Atoi(row[1])
		queryMap[uint32(label)] = q
		row, err = reader.ReadRow()
	}
}

func createDriver() {
	auth := neo4j.BasicAuth(username, password, "")
	var err error
	driver, err = neo4j.NewDriverWithContext(uri, auth)
	if err != nil {
		panic(err)
	}
}

func addData(relations []edge) bool {
	ctx := context.Background()
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: DB_NAME})
	defer session.Close(ctx)
	tx, err := session.BeginTransaction(ctx)
	if err != nil {
		return false
	}
	for _, rel := range relations {
		q := queryMap[rel.label]
		queryMap := map[string]any{"id1": rel.src, "id2": rel.dest}
		_, err := tx.Run(ctx, q, queryMap)
		if err != nil {
			fmt.Println(err)
			tx.Rollback(ctx)
			return false
		}
	}
	err = tx.Commit(ctx)
	if err != nil {
		fmt.Println(err)
		tx.Rollback(ctx)
		return false
	}
	return true
}

func createIndex() {
	v := "CREATE INDEX uidIndex IF NOT EXISTS FOR (t:NODE) ON (t.uid)"
	ctx := context.Background()
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: DB_NAME})
	defer session.Close(ctx)
	_, err := session.Run(ctx, v, nil)
	if err != nil {
		panic(err)
	}
}
