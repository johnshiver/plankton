package main

import (
	"log"

	"github.com/johnshiver/plankton/scheduler"
)

func main() {
	ratings_csv_loader := NewCSVExtractor("ratings.csv")
	ratings_pg_loader := NewPostgresLoader()
	ratings_pg_loader.AddChildren(ratings_csv_loader)

	my_scheduler, err := scheduler.NewTaskScheduler("PostgresExample", ratings_pg_loader, true)
	if err != nil {
		log.Panic(err)
	}
	my_scheduler.Start()

}
