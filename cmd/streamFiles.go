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
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// FileStreamJob .
type FileStreamJob struct {
	URL    string `mapstructure:"url"`
	Bucket string `mapstructure:"bucket"`
}

// // FileStreamJobs .
// type FileStreamJobs struct {

// }

// streamFilesCmd represents the streamFiles command
var streamFilesCmd = &cobra.Command{
	Use:   "streamFiles",
	Short: "A brief description of your command",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("streamFiles called")
		var fileStreamJobs []FileStreamJob

		err := viper.UnmarshalKey("file_stream_jobs", &fileStreamJobs)
		if err != nil {
			panic(err)
		}
		for _, fileStreamJob := range fileStreamJobs {
			fmt.Printf("Streaming file from URL %s to the S3 bucket %s\n", fileStreamJob.URL, fileStreamJob.Bucket)
		}
	},
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
