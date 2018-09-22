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

}

func TestRecreateStoredDag(t *testing.T) {

}
