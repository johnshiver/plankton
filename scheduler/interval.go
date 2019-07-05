package scheduler

import "time"

// SchedulerRange
type SchedulerRange struct {
	StartsAt string
	EndsAt   string
	Cadence  time.Duration
}

func validateSchedulerRange(sr *SchedulerRange) (bool, error) {

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
			return false, nil
		}

	}

	return true, nil

}

func (ts *TaskScheduler) GetNextRange() (*SchedulerRange, error) {
	return &SchedulerRange{}, nil

}
