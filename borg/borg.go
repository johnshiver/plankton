package borg

import (
	"log"
	"time"

	"github.com/johnshiver/plankton/config"
	"github.com/johnshiver/plankton/scheduler"
	"github.com/robfig/cron"
	"gopkg.in/natefinch/lumberjack.v2"
)

const BORG_SCHEDULER_LOG_FILE = "plankton_borg.log"

type AssimilatedScheduler struct {
	Scheduler    *scheduler.TaskScheduler
	ScheduleSpec string
	Logger       *log.Logger
}

func (as *AssimilatedScheduler) RunScheduler() {
	if as.Scheduler.Status == scheduler.RUNNING {
		as.Logger.Printf("Scheduler %s is still running, skipping scheduled run\n", as.Scheduler.Name)
		return
	}
	as.Logger.Printf("Starting scheduler.")
	as.Scheduler.Start()
	as.Logger.Printf("Scheduler finished.")
}

type BorgTaskScheduler struct {
	Schedulers []AssimilatedScheduler
	Cron       *cron.Cron
	Logger     *log.Logger
}

func NewBorgTaskScheduler(schedulers ...AssimilatedScheduler) (*BorgTaskScheduler, error) {
	schedulerCron := cron.New()
	assimilatedSchedulers := []AssimilatedScheduler{}
	logConfig := &lumberjack.Logger{
		Filename:   GetLogFileName(),
		MaxSize:    500, // megabytes
		MaxBackups: 3,
		MaxAge:     28,   //days
		Compress:   true, // disabled by default
	}

	borgLogger := log.New(logConfig, "borg-", log.LstdFlags)
	borgScheduler := BorgTaskScheduler{
		assimilatedSchedulers,
		schedulerCron,
		borgLogger,
	}
	for _, scheduler := range schedulers {
		scheduler.Logger = log.New(logConfig, scheduler.Scheduler.Name+"-", log.LstdFlags)
		borgScheduler.Schedulers = append(borgScheduler.Schedulers, scheduler)
		borgScheduler.Cron.AddFunc(scheduler.ScheduleSpec, scheduler.RunScheduler)
	}
	return &borgScheduler, nil
}

func (bs *BorgTaskScheduler) Start() {
	bs.Cron.Start()
	defer bs.Cron.Stop()

	keepRunning := true
	ticker := time.NewTicker(time.Second * 5)
	for keepRunning {
		select {
		case <-ticker.C:
			for _, s := range bs.Schedulers {
				bs.Logger.Printf("%s -> %s\n", s.Scheduler.Name, s.Scheduler.Status)
			}
		}
	}
}

func GetLogFileName() string {
	c := config.GetConfig()
	loggingFile := c.LoggingDirectory + BORG_SCHEDULER_LOG_FILE
	return loggingFile
}
