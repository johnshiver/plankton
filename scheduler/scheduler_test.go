package scheduler

import (
	"testing"

	"github.com/johnshiver/plankton/config"
	"github.com/johnshiver/plankton/task"
)

// TODO: make this test task stuff shared
type TestTask struct {
	*Task

	// TODO: make the test task_param fields more descriptive
	N int    `task_param:""`
	X string `task_param:""`
	Z int
}

func (tt *TestTask) Run() {
	return
}
func (tt *TestTask) GetTask() *Task {
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

func init() {
	c := config.GetConfig()
	c.SetDatabaseConfig(config.TEST_DATABASE)
	c.ReadAndSetConfig()
}

func TestSaveSchedulerDag(t *testing.T) {
	t1 := createTestTaskRunner("t1", "test1", 0)
	t2 := createTestTaskRunner("t2", "test2", 1)
	t3 := createTestTaskRunner("t3", "test3", 2)
	t1.GetTask().AddChildren(t2, t3)
	test_scheduler, err := NewTaskScheduler(t1, true)
	if err != nil {
		return err
	}
	test_scheduler.Start()
	// TODO: add methods on scheduler to retrieve info from database

}

func TestRecreateStoredDag(t *testing.T) {

}
