package scheduler

import (
	"errors"
	"time"

	"github.com/johnshiver/plankton/config"
)

// SchedulerRange
type SchedulerRange struct {
	StartsAt string
	EndsAt   string
	Cadence  string
}

func validateSchedulerRange(sr *SchedulerRange) (bool, error) {
	cadenceDuration, err := time.ParseDuration(sr.Cadence)
	if err != nil {
		return false, err
	}

	if cadenceDuration < time.Hour {
		return false, errors.New("Cadence must be at least one hour")
	}
	startsAt, err := time.Parse(time.RFC3339, sr.StartsAt)
	if err != nil {
		return false, err
	}
	// EndsAt can be empty, implies that it should run indefinitely
	if len(sr.EndsAt) > 0 {
		endsAt, err := time.Parse(time.RFC3339, sr.EndsAt)
		if err != nil {
			return false, err
		}

		// startsAt should be before endsAt, and the values should not be equal
		if endsAt.Unix() <= startsAt.Unix() {
			return false, errors.New("StartsAt must come before EndsAt")
		}
	}
	return true, nil

}

type schedulerRun struct {
	start string
	end   string
}

func (ts *TaskScheduler) GetAllRuns() []schedulerRun {
	c := config.GetConfig()
	var results []schedulerRun
	c.DataBase.Table("plankton_records").
		Select("min(starts_at) as start, max(ends_at) as end").
		Where("scheduler_name = ?", ts.Name).
		Group("scheduler_uuid").
		Order("ended_at asc").
		Scan(&results)
	return results

}

// GetNextRange
//
// Returns Next Range of time that is ready to run
func (ts *TaskScheduler) GetNextRange() (string, string, error) {
	// var sr *SchedulerRange
	// if there are no records
	// StartsAt = StartsAt - Cadence
	// EndsAt = StartsAt

	// if there are more than one record, compare what has run to what should have run
	// and take the first range available in ascending order

	return "", "", nil
}

func (ts *TaskScheduler) IsCalculatedIntervalScheduler() bool {
	return !(*ts.CoversTimeRange == SchedulerRange{})
}
