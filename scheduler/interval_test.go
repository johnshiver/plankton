package scheduler

import (
	"testing"
)

func TestValidSchedulerIntervalString(t *testing.T) {
	t1 := createTestTaskRunner("test1", "", 0)
	_, err := NewTaskScheduler("TestScheduler", "", t1, true, &SchedulerRange{
		StartsAt: "2019-01-02T12:10:00Z",
		EndsAt:   "2019-01-03T12:10:00Z",
		Cadence:  "24h",
	})
	if err != nil {
		t.Error("NewTaskScheduler returned error when there should be none", err)
	}
}

func TestValidateRange(t *testing.T) {
	var intervalStringTests = []struct {
		testRange *SchedulerRange
		expected  bool
	}{
		// valid format
		{&SchedulerRange{StartsAt: "2019-01-01T00:00:00Z", EndsAt: "2019-01-01T00:00:10Z", Cadence: "24h"}, true},
		// invalid format
		{&SchedulerRange{StartsAt: "2019-01-01", EndsAt: "2019-01-02", Cadence: "24h"}, false},
		// completely invalid string
		{&SchedulerRange{StartsAt: "this shouldnt work", EndsAt: "neither should this", Cadence: "24h"}, false},
		// valid format, same time
		{&SchedulerRange{StartsAt: "2019-01-01T00:00:00Z", EndsAt: "2019-01-01T00:00:00Z", Cadence: "24h"}, false},
		// valid format, start after ends
		{&SchedulerRange{StartsAt: "2019-01-01T00:00:00Z", EndsAt: "2018-01-01T00:00:00Z", Cadence: "24h"}, false},
		// EndsAt can be ""
		{&SchedulerRange{StartsAt: "2019-01-01T00:00:00Z", EndsAt: "", Cadence: "24h"}, true},
		// StartsAt must be defined
		{&SchedulerRange{StartsAt: "", EndsAt: "2019-01-01T00:00:00Z", Cadence: "24h"}, false},
		// Cadence must be at least an hour
		{&SchedulerRange{StartsAt: "2019-01-01T00:00:00Z", EndsAt: "2019-01-01T00:00:10Z", Cadence: "5m"}, false},
	}

	for _, test := range intervalStringTests {
		valid, err := validateSchedulerRange(test.testRange)
		if valid != test.expected {
			t.Errorf("Range %s -> %s, got %v wanted %v, err: %v", test.testRange.StartsAt, test.testRange.EndsAt, valid, test.expected, err)
		}
	}
}

func TestIsCalculatedIntervalScheduler(t *testing.T) {
	t1 := createTestTaskRunner("test1", "", 0)
	s1, err := NewTaskScheduler("TestScheduler1", "", t1, true, &SchedulerRange{
		StartsAt: "2019-01-02T12:10:00Z",
		EndsAt:   "2019-01-03T12:10:00Z",
		Cadence:  "24h",
	})
	if err != nil {
		t.Error(err)
	}
	s2, err := NewTaskScheduler("TestScheduler2", "", t1, true, &SchedulerRange{})
	if err != nil {
		t.Error(err)
	}

	var calculatedIntervalTests = []struct {
		testScheduler *TaskScheduler
		expected      bool
	}{
		{s1, true},
		{s2, false},
	}

	for _, test := range calculatedIntervalTests {
		if test.testScheduler.IsCalculatedIntervalScheduler() != test.expected {
			t.Errorf("%s returned %v, expected %v", test.testScheduler.Name, test.testScheduler.IsCalculatedIntervalScheduler(), test.expected)
		}
	}
}

func TestGetNextRange(t *testing.T) {
}

// Tests that scheduler correctly sets interval on its tasks
func TestSetInterval(t *testing.T) {

}
