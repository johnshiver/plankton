package main

import (
	"log"

	"github.com/johnshiver/plankton/scheduler"
)

func main() {
	ratings_csv_loader := NewCSVExtractor("/Users/js231813/go/src/github.com/johnshiver/plankton/example/postgres_example/the-movies-dataset/ratings.csv")
	ratings_pg_loader := NewPostgresLoader()
	ratings_pg_loader.AddChildren(ratings_csv_loader)

	my_scheduler, err := scheduler.NewTaskScheduler(ratings_pg_loader, true)
	if err != nil {
		log.Panic(err)
	}
	my_scheduler.Start()

}
