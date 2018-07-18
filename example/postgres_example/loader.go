package main

import (
	"encoding/json"
	"log"
	"strconv"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"

	"github.com/johnshiver/plankton/task"
)

type PostgresLoader struct {
	table_name string `task_param:""`
	task       *task.Task
}

type Rating struct {
	gorm.Model
	user_id  int
	movie_id int
	rating   float64
}

func (pl *PostgresLoader) Run() {

	db, err := gorm.Open("postgres", "host=127.0.0.1 port=5432 user=postgres dbname=mytestdb password=mysecretpassword sslmode=disable")

	if err != nil {
		panic("failed to connect database")
	}
	defer db.Close()

	db.AutoMigrate(&Rating{})

	for result := range pl.GetTask().ResultsChannel {
		results := []byte(result)
		var new_line []string

		err := json.Unmarshal(results, &new_line)
		if err != nil {
			log.Panic(err)
		}

		user_id, _ := strconv.Atoi(new_line[0])
		movie_id, _ := strconv.Atoi(new_line[1])
		rating, _ := strconv.ParseFloat(new_line[2], 64)

		db.Create(&Rating{
			user_id:  user_id,
			movie_id: movie_id,
			rating:   rating,
		})

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
