package borg

import (
	"log"
	"time"

	"github.com/johnshiver/plankton/scheduler"
	"github.com/robfig/cron"
)

type AssimilatedScheduler struct {
	Scheduler    *scheduler.TaskScheduler
	ScheduleSpec string
}

func (as *AssimilatedScheduler) RunScheduler() {
	if as.Scheduler.Status == scheduler.RUNNING {
		log.Printf("Scheduler %s is still running, skipping scheduled run\n", as.Scheduler.Name)
		return
	}
	log.Printf("Starting %s\n", as.Scheduler.Name)
	as.Scheduler.Start()
	log.Printf("Finished %s\n", as.Scheduler.Name)
}

type BorgTaskScheduler struct {
	Schedulers []AssimilatedScheduler
	Cron       *cron.Cron
}

func NewBorgTaskScheduler(schedulers ...AssimilatedScheduler) (*BorgTaskScheduler, error) {
	schedulerCron := cron.New()
	assimilatedSchedulers := []AssimilatedScheduler{}
	borgScheduler := BorgTaskScheduler{
		assimilatedSchedulers,
		schedulerCron,
	}
	for _, scheduler := range schedulers {
		borgScheduler.Schedulers = append(borgScheduler.Schedulers, scheduler)
		borgScheduler.Cron.AddFunc(scheduler.ScheduleSpec, scheduler.RunScheduler)
	}
	return &borgScheduler, nil
}

func (bs *BorgTaskScheduler) Start() {
	bs.Cron.Start()
	keepRunning := true
	ticker := time.NewTicker(time.Second * 5)
	for keepRunning {
		select {
		case <-ticker.C:
			for _, s := range bs.Schedulers {
				log.Printf("%s -> %s\n", s.Scheduler.Name, s.Scheduler.Status)
			}
		}
	}
}
