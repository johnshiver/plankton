package terminal

import (
	"sync"
	"time"

	"github.com/johnshiver/plankton/borg"
	"github.com/johnshiver/plankton/scheduler"
	"github.com/rivo/tview"
)

var app = tview.NewApplication()

var mu *sync.Mutex
var currentTaskScheduler *scheduler.TaskScheduler
var borgScheduler *borg.BorgTaskScheduler

func init() {
	mu = &sync.Mutex{}
}

func SetCurrentTaskScheduler(newScheduler *scheduler.TaskScheduler) {
	mu.Lock()
	defer mu.Unlock()
	currentTaskScheduler = newScheduler
}

func GetCurrentTaskScheduler() *scheduler.TaskScheduler {
	mu.Lock()
	defer mu.Unlock()
	return currentTaskScheduler
}

func GetBorgScheduler() *borg.BorgTaskScheduler {
	mu.Lock()
	defer mu.Unlock()
	return borgScheduler
}

func SetBorgScheduler(bs *borg.BorgTaskScheduler) {
	mu.Lock()
	defer mu.Unlock()
	borgScheduler = bs
}

func RunTerminal(bs *borg.BorgTaskScheduler) {

	if len(bs.Schedulers) < 1 {
		bs.Logger.Fatal("Couldnt run terminal, no schedulers!")
		return
	}

	SetBorgScheduler(bs)
	if len(bs.Schedulers) < 1 {
		panic("Cannot start terminal with no schedulers")

	}
	SetCurrentTaskScheduler(bs.Schedulers[0])
	schedulerView := CreateSelectSchedulerView()

	// Create the main layout.
	layout := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(schedulerView, 0, 1, true)

	// refresh UI every 5 seconds
	go func() {
		ticker := time.NewTicker(time.Second * 5)
		table := GetTableView()
		for {
			select {
			case <-ticker.C:
				SetTableCells(table)
			}
		}
	}()

	// Start the application.
	if err := app.SetRoot(layout, true).Run(); err != nil {
		panic(err)
	}

}
