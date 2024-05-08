package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/adityachandla/ldbc_converter/file_util"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var (
	src  = flag.String("src", "", "Source directory")
	dest = flag.String("dest", "", "Destination: onezone/general")
)

var client *s3.Client

const ozBucket = "s3graphtest10oz--use1-az6--x-s3"
const generalBucket = "s3graphtest10"

func main() {
	flag.Parse()
	if *src == "" || *dest == "" {
		fmt.Println("Input directory and destination bucket required")
		return
	}
	if !strings.HasSuffix(*src, "/") {
		*src += "/"
	}
	var bucket string
	if *dest == "onezone" {
		bucket = ozBucket
	} else if *dest == "general" {
		bucket = generalBucket
	} else {
		fmt.Println("Invalid destination")
		return
	}
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("us-east-1"))
	if err != nil {
		panic(err)
	}

	client = s3.NewFromConfig(cfg)
	deleteBucket(bucket)
	copyFiles(*src, bucket)
}

func copyFiles(inDir string, bucket string) {
	files, err := file_util.GetFilesInDir(inDir)
	if err != nil {
		panic(err)
	}
	var wg sync.WaitGroup
	wg.Add(10)
	fileChannel := make(chan string)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			upload(bucket, inDir, fileChannel)
		}()
	}
	for _, file := range files {
		fileChannel <- file
	}
	close(fileChannel)
	wg.Wait()
}

func upload(bucket string, directory string, files <-chan string) {
	for file := range files {
		f, err := os.Open(directory + file)
		if err != nil {
			panic(err)
		}
		putInput := &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(file),
			Body:   f,
		}
		_, err = client.PutObject(context.TODO(), putInput)
		if err != nil {
			fmt.Println("Unable to upload file")
		}
		f.Close()
	}
}

func deleteBucket(bucket string) {
	request := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	}
	response, err := client.ListObjectsV2(context.TODO(), request)
	if err != nil {
		panic(err)
	}
	var wg sync.WaitGroup
	wg.Add(len(response.Contents))
	for _, object := range response.Contents {
		key := object.Key
		go func() {
			defer wg.Done()
			deleteRequest := &s3.DeleteObjectInput{
				Bucket: aws.String(bucket),
				Key:    key,
			}
			_, err := client.DeleteObject(context.TODO(), deleteRequest)
			if err != nil {
				fmt.Println("Unable to delete object")
			}
		}()
	}
	wg.Wait()
}
