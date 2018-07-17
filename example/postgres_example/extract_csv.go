package main

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"os"

	"github.com/johnshiver/plankton/task"
)

type CSVTaskRunner struct {
	csv_file string `task_param:""`
	task     *task.Task
}

func (cv *CSVTaskRunner) Run() {

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
			//			log.Fatal(err)
			log.Println(err)
		}

		lines, _ := json.Marshal(record)

		//lines := strings.Join(record, "   ")
		cv.GetTask().Parent.GetTask().ResultsChannel <- string(lines)
	}

}

func (cv *CSVTaskRunner) GetTask() *task.Task {
	return cv.task
}

func NewCSVRunner(csv_file string) *CSVTaskRunner {
	task := task.NewTask(
		"CSVExtractor",
	)
	return &CSVTaskRunner{
		csv_file: csv_file,
		task:     task,
	}
}
