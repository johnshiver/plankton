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
	newRunner := TestTask{
		task.NewTask(name),
		n,
		x,
		0,
	}
	return &newRunner

}

var testConfig config.Config

/*
Function that can do setup / tear down for tests

https://golang.org/pkg/testing/#hdr-Main
*/
func TestMain(m *testing.M) {
	config.SetDatabaseConfig(config.TEST_SQLITE_DATABASE)
	testConfig = config.GetConfig()
	testConfig.DataBase.AutoMigrate(PlanktonRecord{})
	exitCode := m.Run()

	testConfig.DataBase.DropTable(&PlanktonRecord{})
	os.Exit(exitCode)
}

func TestSaveSchedulerDag(t *testing.T) {
	t1 := createTestTaskRunner("t1", "test1", 0)
	t2 := createTestTaskRunner("t2", "test2", 1)
	t3 := createTestTaskRunner("t3", "test3", 2)
	t1.GetTask().AddChildren(t2, t3)
	// create scheduler that doesnt print to standard out
	testScheduler, err := NewTaskScheduler("TestScheduler", "", t1, true)
	if err != nil {
		t.Errorf("Received error from task scheduler %v", err)
	}
	testScheduler.Start()

	var records []PlanktonRecord
	testConfig.DataBase.Where("scheduler_uuid = ?", testScheduler.uuid.String()).Find(&records)
	if len(records) < 1 {
		t.Errorf("Failed to create test records")
	}

	recordMap := make(map[string]PlanktonRecord)
	for _, record := range records {
		recordMap[record.TaskName] = record
	}

	// test params
	var paramTests = []struct {
		input string
		want  string
	}{
		{"t1", t1.GetSerializedParams()},
		{"t2", t2.GetSerializedParams()},
		{"t3", t3.GetSerializedParams()},
	}
	for _, test := range paramTests {
		record := recordMap[test.input]
		if record.TaskParams != test.want {
			t.Errorf("%s record stored %s, wanted %s", test.input, record.TaskParams, test.want)
		}
	}

	// test hashes
	var hashTests = []struct {
		input string
		want  string
	}{
		{"t1", t1.GetHash()},
		{"t2", t2.GetHash()},
		{"t3", t3.GetHash()},
	}
	for _, test := range hashTests {
		record := recordMap[test.input]
		if record.TaskHash != test.want {
			t.Errorf("%s record stored %s, wanted %s", test.input, record.TaskHash, test.want)
		}
	}

	// test parent hashes
	var parentTests = []struct {
		input string
		want  string
	}{
		{"t1", ""},
		{"t2", t1.GetHash()},
		{"t3", t1.GetHash()},
	}
	for _, test := range parentTests {
		record := recordMap[test.input]
		if record.ParentHash != test.want {
			t.Errorf("%s record stored %s, wanted %s", test.input, record.TaskHash, test.want)
		}
	}
}

func TestCompareTwoTaskDagsForEquality(t *testing.T) {
	// test dag 1
	t1 := createTestTaskRunner("t1", "test1", 0)
	t2 := createTestTaskRunner("t2", "test2", 1)
	t3 := createTestTaskRunner("t3", "test3", 2)
	t3.AddChildren(t2, t1)

	// test dag 2, should be equivalent to test dag 1
	t4 := createTestTaskRunner("t1", "test1", 0)
	t5 := createTestTaskRunner("t2", "test2", 1)
	t6 := createTestTaskRunner("t3", "test3", 2)
	t6.AddChildren(t5, t4)

	// test dag 3, not equivalent to dag 1 for names
	t7 := createTestTaskRunner("t10000000", "test1", 0)
	t8 := createTestTaskRunner("t2", "test2", 1)
	t9 := createTestTaskRunner("t3", "test3", 2)
	t9.AddChildren(t8, t7)

	// test dag 4, not equivalent to dag 1 for string params
	t10 := createTestTaskRunner("t1", "test1000000000", 0)
	t11 := createTestTaskRunner("t2", "test2", 1)
	t12 := createTestTaskRunner("t3", "test3", 2)
	t12.AddChildren(t11, t10)

	// test dag 5, not equivalent to dag 1 for int params
	t13 := createTestTaskRunner("t1", "test1", 10000000000000)
	t14 := createTestTaskRunner("t2", "test2", 1)
	t15 := createTestTaskRunner("t3", "test3", 2)
	t15.AddChildren(t14, t13)

	if !AreTaskDagsEqual(t3, t6) {
		t.Errorf("Tasks dags werent equal, equality check failed")
	}

	if AreTaskDagsEqual(t3, t9) {
		t.Errorf("Tasks dags should NOT have been equal, different task names")
	}

	if AreTaskDagsEqual(t3, t12) {
		t.Errorf("Tasks dags should NOT have been equal, different string params")
	}
	if AreTaskDagsEqual(t3, t15) {
		t.Errorf("Tasks dags should NOT have been equal, different int params")
	}

}

func TestRecreateStoredDag(t *testing.T) {
	t1 := createTestTaskRunner("t1", "test1", 0)
	t2 := createTestTaskRunner("t2", "test2", 1)
	t3 := createTestTaskRunner("t3", "test3", 2)
	t1.GetTask().AddChildren(t2, t3)
	// create scheduler that doesnt print to standard out
	testScheduler, err := NewTaskScheduler("TestScheduler", "", t1, true)
	if err != nil {
		t.Errorf("Received error from task scheduler %v", err)
	}
	testScheduler.Start()
	firstSchedulerUUID := testScheduler.uuid.String()

	revivedTest1 := createTestTaskRunner("", "999999", 0)
	revivedTest2 := createTestTaskRunner("", "", 0)
	revivedTest3 := createTestTaskRunner("", "", 0)
	revivedTest3.AddChildren(revivedTest1, revivedTest2)

	if AreTaskDagsEqual(t1, revivedTest3) {
		t.Errorf("Task dags are equal, something went wrong")
	}

	err = ReCreateStoredDag(revivedTest3, firstSchedulerUUID)
	if err != nil {
		t.Error(err)
	}

	if !AreTaskDagsEqual(t1, revivedTest3) {
		t.Errorf("Task dags were not equal, recreation of stored dag failed")
		// spew.Dump(revivedTest3)
		//spew.Dump(firstSchedulerUUID )
	}

}
