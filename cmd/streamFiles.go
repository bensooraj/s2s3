/*
Copyright © 2021 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"archive/tar"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// FileStreamJob .
type FileStreamJob struct {
	URL    string `mapstructure:"url"`
	Bucket string `mapstructure:"bucket"`
	Folder string `mapstructure:"folder"`
	Untar  bool   `mapstructure:"untar"`
	JobID  int    `mapstructure:",omitempty"`
}

// streamFilesCmd represents the streamFiles command
var streamFilesCmd = &cobra.Command{
	Use:   "streamFiles",
	Short: "A brief description of your command",
	RunE: func(cmd *cobra.Command, args []string) error {
		log.Println("s2s3 streamFiles started")
		var fileStreamJobs []FileStreamJob

		err := viper.UnmarshalKey("file_stream_jobs", &fileStreamJobs)
		if err != nil {
			return err
		}

		fileStreamJobChannel := make(chan FileStreamJob, numberOfParallelJobs)
		errorChannel := make(chan error, numberOfParallelJobs)
		doneChannel := make(chan struct{}, 0)

		var wg sync.WaitGroup
		for i := 0; i < numberOfParallelJobs; i++ {
			go fileStreamUploader(i, &wg, doneChannel, fileStreamJobChannel, errorChannel)
		}

		errorChannel <- errors.New("Test Error")
		go func() {
			for {
				select {
				case <-doneChannel:
					log.Println("[CLEANUP] Exiting error logger routine")
					return
				case err := <-errorChannel:
					log.Println("[ERROR] ", err)
				}
			}
		}()

		numJobs := len(fileStreamJobs)
		wg.Add(numJobs)
		for i := 0; i < numJobs; i++ {
			fileStreamJob := fileStreamJobs[i]
			fileStreamJob.JobID = i
			fileStreamJobChannel <- fileStreamJob
		}

		wg.Wait()
		close(doneChannel)
		<-time.After(7 * time.Second)

		return nil
	},
}

func fileStreamUploader(workerID int, wg *sync.WaitGroup, doneChannel <-chan struct{}, fileStreamJobChannel <-chan FileStreamJob, errorChannel chan<- error) {
	for {
		select {
		case <-doneChannel:
			log.Println("[CLEANUP] Exiting fileStreamUploader worker", workerID)
			return
		case fileStreamJob := <-fileStreamJobChannel:
			log.Printf("[%d START] Started uploading %s\n", fileStreamJob.JobID, fileStreamJob.URL)
			log.Printf("[%d] Streaming file from URL %s to the S3 bucket %s\n", workerID, fileStreamJob.URL, fileStreamJob.Bucket)

			req, err := http.NewRequest("GET", fileStreamJob.URL, nil)
			if err != nil {
				errorChannel <- err
				break
			}
			client := &http.Client{}
			resp, err := client.Do(req)
			if err != nil {
				errorChannel <- err
				break
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				awsConfig := aws.
					NewConfig().
					WithCredentials(credentials.NewStaticCredentials(
						awsCredentials["aws_access_key_id"],
						awsCredentials["aws_secret_access_key"],
						"",
					)).
					WithRegion(awsCredentials["aws_default_region"])

				sess, err := session.NewSession(awsConfig)
				if err != nil {
					errorChannel <- err
					break
				}

				log.Println("resp.ContentLength: ", resp.ContentLength)
				log.Println("resp.Header: ", resp.Header)

				uploader := s3manager.NewUploader(sess, func(u *s3manager.Uploader) {
					u.Concurrency = 5
					u.PartSize = 10 * 1024 * 1024 // The minimum/default allowed part size is 5MB
				})

				if fileStreamJob.Untar {
					uncompressedGzipStream, err := gzip.NewReader(resp.Body)
					if err != nil {
						log.Fatal("gzip: NewReader failed: ", err)
						errorChannel <- err
						break
					}
					// https://filebin.net/1vu3jjqj4cker991/prku_0307.tar.gz?t=korc1n0e
					tarReader := tar.NewReader(uncompressedGzipStream)
					for {
						header, err := tarReader.Next()

						if err == io.EOF {
							log.Printf("[%d END] EOF for %s\n", fileStreamJob.JobID, fileStreamJob.URL)
							break
						}
						if err != nil {
							log.Fatal("tar: Next seek failed", err)
							errorChannel <- err
							break
						}

						switch header.Typeflag {
						case tar.TypeDir:
							// No action for now
							log.Println("Directory found. Skipping.")

						case tar.TypeReg:

							fileName := strings.TrimSuffix(header.Name, filepath.Ext(header.Name))
							fileExtension := filepath.Ext(header.Name)
							log.Println("Header file name: ", fileName)
							log.Println("Header file extension: ", fileExtension)

							var keyName string
							if fileStreamJob.Folder != "" {
								keyName = fmt.Sprintf("%s/%s%s", fileStreamJob.Folder, fileName, fileExtension)
							} else {
								keyName = fmt.Sprintf("%s%s", fileName, fileExtension)
							}
							_, err = uploader.Upload(&s3manager.UploadInput{
								Bucket: aws.String(fileStreamJob.Bucket),
								Key:    aws.String(keyName),
								Body:   tarReader,
								// ContentType: &format,
							})

						default:
							log.Fatalf("ExtractTarGz: uknown type: %v in %s", header.Typeflag, header.Name)
						}
					}
					wg.Done()
				} else {
					var keyName string
					fileName := path.Base(fileStreamJob.URL)

					if fileStreamJob.Folder != "" {
						keyName = fmt.Sprintf("%s/%s", fileStreamJob.Folder, fileName)
					} else {
						keyName = fmt.Sprintf("%s", fileName)
					}
					_, err = uploader.Upload(&s3manager.UploadInput{
						Bucket: aws.String(fileStreamJob.Bucket),
						Key:    aws.String(keyName),
						Body:   resp.Body,
						// ContentType: &format,
					})
					log.Printf("[%d END] Completed uploading %s\n", fileStreamJob.JobID, fileStreamJob.URL)
					wg.Done()
				}
			} else {
				errorChannel <- fmt.Errorf("%d::%s", resp.StatusCode, resp.Status)
				log.Printf("[%d END] %d::%s Error uploading %s\n", fileStreamJob.JobID, resp.StatusCode, resp.Status, fileStreamJob.URL)
				wg.Done()
			}
		}
	}

}

func init() {
	rootCmd.AddCommand(streamFilesCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// streamFilesCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// streamFilesCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
