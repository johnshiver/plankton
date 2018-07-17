package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/johnshiver/plankton/task"
)

type PostgresLoader struct {
	table_name string `task_param:""`
	task       *task.Task
}

func (pl *PostgresLoader) Run() {

	for result := range pl.GetTask().ResultsChannel {
		//		fmt.Println(result)
		results := []byte(result)
		var new_line []string

		err := json.Unmarshal(results, &new_line)
		if err != nil {
			log.Panic(err)
		}
		fmt.Println(new_line[0])
	}

}

func (pl *PostgresLoader) GetTask() *task.Task {
	return pl.task
}

func NewPostgresLoader(table_name string) *PostgresLoader {
	task := task.NewTask(
		"PostgresLoader",
	)
	return &PostgresLoader{
		table_name: table_name,
		task:       task,
	}
}
