package scheduler

import (
	"os"
	"testing"

	"github.com/johnshiver/plankton/config"
	"github.com/johnshiver/plankton/task"
)

// TODO: make this test task stuff shared
type TestTask struct {
	*task.Task

	// TODO: make the test task_param fields more descriptive
	N int    `task_param:""`
	X string `task_param:""`
	Z int
}

func (tt *TestTask) Run() {
	return
}
func (tt *TestTask) GetTask() *task.Task {
	return tt.Task
}

func compareTestTaskParams(a, b *TestTask) bool {
	if a.N == b.N && a.X == b.X {
		return true
	}
	return false
}

func createTestTaskRunner(name, x string, n int) *TestTask {
	new_runner := TestTask{
		task.NewTask(name),
		n,
		x,
		0,
	}
	return &new_runner

}

var test_config config.Config

/*
Function that can do setup / tear down for tests

https://golang.org/pkg/testing/#hdr-Main
*/
func TestMain(m *testing.M) {
	config.SetDatabaseConfig(config.TEST_SQLITE_DATABASE)
	test_config = config.GetConfig()

	exit_code := m.Run()

	test_config.DataBase.DropTable(&PlanktonRecord{})
	os.Exit(exit_code)
}

func TestSaveSchedulerDag(t *testing.T) {
	t1 := createTestTaskRunner("t1", "test1", 0)
	t2 := createTestTaskRunner("t2", "test2", 1)
	t3 := createTestTaskRunner("t3", "test3", 2)
	t1.GetTask().AddChildren(t2, t3)
	// create scheduler that doesnt print to standard out
	test_scheduler, err := NewTaskScheduler(t1, true)
	if err != nil {
		t.Errorf("Received error from task scheduler %v", err)
	}
	test_scheduler.Start()
	var records []PlanktonRecord
	test_config.DataBase.Where("scheduler_uuid = ?", test_scheduler.uuid.String()).Find(&records)
	if len(records) < 1 {
		t.Errorf("Failed to create test records")
	}

	record_map := make(map[string]PlanktonRecord)
	for _, record := range records {
		record_map[record.TaskName] = record
	}

	// test params
	var param_tests = []struct {
		input string
		want  string
	}{
		{"t1", t1.GetSerializedParams()},
		{"t2", t2.GetSerializedParams()},
		{"t3", t3.GetSerializedParams()},
	}
	for _, test := range param_tests {
		record := record_map[test.input]
		if record.TaskParams != test.want {
			t.Errorf("%s record stored %s, wanted %s", test.input, record.TaskParams, test.want)
		}
	}

	// test hashes
	var hash_tests = []struct {
		input string
		want  string
	}{
		{"t1", t1.GetHash()},
		{"t2", t2.GetHash()},
		{"t3", t3.GetHash()},
	}
	for _, test := range hash_tests {
		record := record_map[test.input]
		if record.TaskHash != test.want {
			t.Errorf("%s record stored %s, wanted %s", test.input, record.TaskHash, test.want)
		}
	}

	// test hashes
	var parent_tests = []struct {
		input string
		want  string
	}{
		{"t1", ""},
		{"t2", t1.GetHash()},
		{"t3", t1.GetHash()},
	}
	for _, test := range parent_tests {
		record := record_map[test.input]
		if record.ParentHash != test.want {
			t.Errorf("%s record stored %s, wanted %s", test.input, record.TaskHash, test.want)
		}
	}
}

func TestRecreateStoredDag(t *testing.T) {

}
