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
	*task.Task
}

type Rating struct {
	gorm.Model
	UserId  int
	MovieId int
	Rating  float64
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

		new_rating := &Rating{
			UserId:  user_id,
			MovieId: movie_id,
			Rating:  rating,
		}

		db.Create(new_rating)
		pl.DataProcessed += 1

	}

}

func (pl *PostgresLoader) GetTask() *task.Task {
	return pl.Task
}

func NewPostgresLoader() *PostgresLoader {
	return &PostgresLoader{
		task.NewTask("PostgresLoader"),
	}
}
