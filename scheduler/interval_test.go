package scheduler

import (
	"testing"
)

func TestValidSchedulerIntervalString(t *testing.T) {
	t1 := createTestTaskRunner("test1", "", 0)
	_, err := NewTaskScheduler("TestScheduler", "", t1, true, &SchedulerRange{
		StartsAt: "2019-01-02T12:10:00Z",
		EndsAt:   "2019-01-03T12:10:00Z",
	})
	if err != nil {
		t.Error("NewTaskScheduler returned error when there should be none", err)
	}
}

func TestValidateRangeString(t *testing.T) {
	var intervalStringTests = []struct {
		testRange *SchedulerRange
		expected  bool
	}{
		// valid format
		{&SchedulerRange{StartsAt: "2019-01-01T00:00:00Z", EndsAt: "2019-01-01T00:00:10Z"}, true},
		// invalid format
		{&SchedulerRange{StartsAt: "2019-01-01", EndsAt: "2019-01-02"}, false},
		// completely invalid string
		{&SchedulerRange{StartsAt: "this shouldnt work", EndsAt: "neither should this"}, false},
		// valid format, same time
		{&SchedulerRange{StartsAt: "2019-01-01T00:00:00Z", EndsAt: "2019-01-01T00:00:00Z"}, false},
		// valid format, start after ends
		{&SchedulerRange{StartsAt: "2019-01-01T00:00:00Z", EndsAt: "2018-01-01T00:00:00Z"}, false},
		// EndsAt can be ""
		{&SchedulerRange{StartsAt: "2019-01-01T00:00:00Z", EndsAt: ""}, true},
		{&SchedulerRange{StartsAt: "", EndsAt: "2019-01-01T00:00:00Z"}, false},
	}

	for _, test := range intervalStringTests {
		valid, err := validateSchedulerRange(test.testRange)
		if valid != test.expected {
			t.Errorf("Range %s -> %s, got %v wanted %v, err: %v", test.testRange.StartsAt, test.testRange.EndsAt, valid, test.expected, err)
		}
	}

}
