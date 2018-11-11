package main

import (
	"log"

	"github.com/johnshiver/plankton/borg"
	"github.com/johnshiver/plankton/scheduler"
	"github.com/johnshiver/plankton/terminal"
)

func main() {
	ratings_csv_loader := NewCSVExtractor("ratings.csv")
	ratings_pg_loader := NewPostgresLoader()
	ratings_pg_loader.AddChildren(ratings_csv_loader)

	my_scheduler, err := scheduler.NewTaskScheduler("PostgresExample", "0 * * * * * ", ratings_pg_loader, true)
	if err != nil {
		log.Panic(err)
	}
	borgScheduler, err := borg.NewBorgTaskScheduler(
		my_scheduler,
	)
	if err != nil {
		log.Panic(err)
	}

	go borgScheduler.Start()
	terminal.RunTerminal(borgScheduler)
}
