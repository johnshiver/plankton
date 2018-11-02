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

type BorgTaskScheduler struct {
	Schedulers []*scheduler.TaskScheduler
	Cron       *cron.Cron
	Logger     *log.Logger
}

func NewBorgTaskScheduler(schedulers ...*scheduler.TaskScheduler) (*BorgTaskScheduler, error) {
	schedulerCron := cron.New()
	assimilatedSchedulers := []*scheduler.TaskScheduler{}
	logConfig := &lumberjack.Logger{
		Filename:   GetLogFileName(),
		MaxSize:    50, // megabytes
		MaxBackups: 3,
		MaxAge:     28,   // days
		Compress:   true, // disabled by default
	}

	borgLogger := log.New(logConfig, "borg-", log.LstdFlags)
	borgScheduler := BorgTaskScheduler{
		assimilatedSchedulers,
		schedulerCron,
		borgLogger,
	}
	for _, s := range schedulers {
		borgScheduler.Schedulers = append(borgScheduler.Schedulers, s)
		borgScheduler.Cron.AddFunc(s.CronSpec, s.Start)
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
				bs.Logger.Printf("%s -> %s\n", s.Name, s.Status())
			}
		}

	}
}

func GetLogFileName() string {
	c := config.GetConfig()
	loggingFile := c.LoggingDirectory + BORG_SCHEDULER_LOG_FILE
	return loggingFile
}
