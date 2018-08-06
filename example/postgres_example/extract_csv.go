package main

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"os"

	"github.com/johnshiver/plankton/task"
)

type CSVExtractor struct {
	*task.Task
	csv_file string `task_param:""`
}

func (cv *CSVExtractor) Run() {

	f, err := os.Open(cv.csv_file)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		lines, _ := json.Marshal(record)
		cv.Parent.GetTask().ResultsChannel <- string(lines)
		cv.DataProcessed += 1
	}

}

func (cv *CSVExtractor) GetTask() *task.Task {
	return cv.Task
}

func NewCSVExtractor(csv_file string) *CSVExtractor {
	return &CSVExtractor{
		task.NewTask("CSVExtractor"),
		csv_file,
	}
}
