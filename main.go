package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
	"gopkg.in/yaml.v2"
)

type Configuration struct {
	Directories []string `yaml:"directories"`
	GoogleCloud struct {
		NameBucket  string `yaml:"nameBucket"`
		PathJSONKey string `yaml:"pathJsonKey"`
	} `yaml:"googleCloud"`
}

var (
	fileConf         string
	conf             Configuration
	totalFilesToCopy int
	filesToCopy      []string

	totalFilesOK    int
	totalFilesError int
)

func usage() {
	fmt.Printf("Usage: %s -conf fileconf.yaml\n", path.Base(os.Args[0]))
	os.Exit(1)
}

func checkFileConf() {
	info, err := os.Stat(fileConf)

	if os.IsNotExist(err) {
		fmt.Printf("[ERROR] File \"%s\" not found\n", fileConf)
		os.Exit(1)
	}

	if info.Size() == 0 {
		fmt.Printf("[ERROR] File \"%s\" is empty\n", fileConf)
		os.Exit(1)
	}
}

func parseFileConf() {
	yamlFile, err := ioutil.ReadFile(fileConf)

	if err != nil {
		fmt.Printf("[ERROR] Reading file configuration: %s\n", err)
		os.Exit(1)
	}

	err = yaml.Unmarshal(yamlFile, &conf)

	if err != nil {
		fmt.Printf("[ERROR] Parsing configuration: %s\n", err)
		os.Exit(1)
	}

	info, err := os.Stat(conf.GoogleCloud.PathJSONKey)

	if os.IsNotExist(err) {
		fmt.Printf("[ERROR] File pathJsonKey \"%s\" not found\n", conf.GoogleCloud.PathJSONKey)
		os.Exit(1)
	}

	if info.Size() == 0 {
		fmt.Printf("[ERROR] File pathJsonKey \"%s\" is empty\n", conf.GoogleCloud.PathJSONKey)
		os.Exit(1)
	}
}

func getFilesToCopy() {
	for i := range conf.Directories {
		_, err := os.Stat(conf.Directories[i])

		if os.IsNotExist(err) {
			fmt.Printf("[WARNING] Dir \"%s\" not found\n", conf.Directories[i])
			continue
		}

		err = filepath.Walk(conf.Directories[i],
			func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}

				if !info.IsDir() {
					filesToCopy = append(filesToCopy, path)
					totalFilesToCopy++
				}

				return nil
			})

		if err != nil {
			fmt.Printf("[ERROR]: %s\n", err)
			os.Exit(1)
		}
	}
}

func copyFiles() {
	var processFilesForGoRoutine int = 20

	var wg sync.WaitGroup
	var mutex sync.Mutex
	var start, end int

	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithCredentialsFile(conf.GoogleCloud.PathJSONKey))

	if err != nil {
		fmt.Printf("[ERROR] %s", err)
		os.Exit(1)
	}

	defer client.Close()

	currentTime := time.Now()

	pathBase := fmt.Sprintf("%d-%02d-%02d_%02d:%02d:%02d", currentTime.Year(),
		currentTime.Month(), currentTime.Day(), currentTime.Hour(), currentTime.Minute(),
		currentTime.Second())

	goRoutines := int(math.Round(float64(totalFilesToCopy) / float64(processFilesForGoRoutine)))

	if goRoutines == 0 {
		goRoutines = 1
	}

	wg.Add(goRoutines)

	for i := 0; i < goRoutines; i++ {
		if goRoutines == 1 {
			// Only one go routine with the complete slice
			start = 0
			end = len(filesToCopy) - 1
		} else {
			if i == 0 {
				// We are at the beginning
				start = 0
				end = processFilesForGoRoutine - 1
			} else if i == goRoutines-1 {
				// We are at the end
				start = end + 1
				end = len(filesToCopy) - 1
			} else {
				start = end + 1
				end = (start + processFilesForGoRoutine) - 1
			}
		}

		go func(start, end int) {
			for n := start; n <= end; n++ {
				path := filesToCopy[n]

				// Double check
				_, err := os.Stat(path)

				if os.IsNotExist(err) {
					fmt.Printf("[WARNING] File \"%s\" not found\n", path)
					continue
				}

				f, err := os.Open(path)

				if err != nil {
					fmt.Errorf("os.Open: %v", err)
					continue
				}

				ctx, cancel := context.WithTimeout(ctx, time.Second*50)

				// Upload the file to the bucket
				wc := client.Bucket(conf.GoogleCloud.NameBucket).Object(pathBase + path).NewWriter(ctx)

				if _, err = io.Copy(wc, f); err != nil {
					fmt.Errorf("io.Copy: %v", err)

					mutex.Lock()
					totalFilesError++
					mutex.Unlock()

					cancel()
					f.Close()

					continue
				}

				if err := wc.Close(); err != nil {
					fmt.Errorf("Writer.Close: %v", err)

					mutex.Lock()
					totalFilesError++
					mutex.Unlock()

					cancel()
					f.Close()

					continue
				}

				fmt.Printf("[OK] File \"%s%s\" copied successfully\n", pathBase, path)

				mutex.Lock()
				totalFilesOK++
				mutex.Unlock()

				cancel()
				f.Close()
			}

			wg.Done()
		}(start, end)
	}

	wg.Wait()

	elapsed := time.Since(currentTime)

	fmt.Printf("\n\nTotal files to copy: %d \n", totalFilesToCopy)
	fmt.Printf("Total files copied: %d \n", totalFilesOK)
	fmt.Printf("Total files with errors: %d \n", totalFilesError)
	fmt.Printf("Copy files took: %v \n", elapsed)
}

func main() {

	if len(os.Args) == 1 {
		usage()
	}

	flag.StringVar(&fileConf, "config", "conf.yaml", "YAML file with the configuration")

	flag.Parse()

	checkFileConf()
	parseFileConf()
	getFilesToCopy()
	copyFiles()

	os.Exit(0)
}
