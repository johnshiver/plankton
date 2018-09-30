package task

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
)

// TODO: make task param flag nicer
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

func createTestTaskRunner(name string, n int) *TestTask {
	new_runner := TestTask{
		NewTask(name),
		n,
		"TestString",
		0,
	}
	CreateAndSetTaskParams(&new_runner)
	return &new_runner

}

func TestVerifyDAG(t *testing.T) {
	// TODO: add more variations of bad dags

	good_dag := createTestTaskRunner("good_dag", 1)
	test2 := createTestTaskRunner("test2", 1)
	test3 := createTestTaskRunner("test3", 1)

	good_dag.GetTask().Children = []TaskRunner{
		test2,
		test3,
	}

	bad_dag := createTestTaskRunner("bad_dag", 1)
	test4 := createTestTaskRunner("test4", 1)
	test5 := createTestTaskRunner("test5", 1)

	bad_dag.GetTask().Children = []TaskRunner{
		test4,
		test5,
	}

	test4.GetTask().Children = []TaskRunner{
		bad_dag,
	}

	var tests = []struct {
		input *Task
		want  bool
	}{
		{good_dag.GetTask(), true},
		{bad_dag.GetTask(), false},
	}
	for _, test := range tests {
		if output := verifyDAG(test.input); output != test.want {
			t.Errorf("VerifyDAG(%v) = %v", test.input, output)
		}
	}

}

// Tests that, given a new task runner, the CreateAndSetTaskParams function correctly
// creates and sets the task params back on the task runner
func TestCreateAndSetTaskParams(t *testing.T) {
	test1 := createTestTaskRunner("test1", 1)

	var tests = []struct {
		input TaskRunner
		want  []*TaskParam
	}{
		{test1, []*TaskParam{
			&TaskParam{
				Name: "N",
				// TODO: this means we need to test getFieldValue too
				//       or determine it doesnt need to be tested
				Data: getFieldValue(test1, "N"),
			},
			&TaskParam{
				Name: "X",
				Data: getFieldValue(test1, "X"),
			},
		},
		},
	}

	for _, test := range tests {
		if output, _ := CreateAndSetTaskParams(test.input); !(reflect.DeepEqual(output, test.want)) {
			t.Errorf("CreateAndSetTaskParams(%v) = %v, wanted: %v", test.input, output, test.want)
		}
	}

}

/*
Check that task runners are serializing their params correctly. Essentially, the same task runner
With different params set should produce unique serializations.  Doesnt check the actual content
of the serialized string, just uniqueness.
*/
func TestGetSerializedParams(t *testing.T) {
	test1 := createTestTaskRunner("test1", 1)
	test2 := createTestTaskRunner("test1", 1)

	test3 := createTestTaskRunner("test3", 2)
	test4 := createTestTaskRunner("test3", 3)

	test5 := createTestTaskRunner("test5", 1)
	test6 := createTestTaskRunner("test6", 1)

	test7 := createTestTaskRunner("test7", 2)
	test8 := createTestTaskRunner("test8", 3)

	var tests = []struct {
		input []TaskRunner
		want  bool
	}{
		{[]TaskRunner{test1, test2}, true},
		{[]TaskRunner{test3, test4}, false},
		{[]TaskRunner{test5, test6}, true},
		{[]TaskRunner{test7, test8}, false},
	}
	for _, test := range tests {
		serialized_task_params := []string{}
		for _, runner := range test.input {
			CreateAndSetTaskParams(runner)
			serialized_task_params = append(serialized_task_params, runner.GetTask().GetSerializedParams())
		}

		s_param1, s_param2 := serialized_task_params[0], serialized_task_params[1]
		result := s_param1 == s_param2

		if result != test.want {
			test_input := spew.Sdump(test.input)
			t.Errorf("GetSerializedParams Failed %s got %v not %v", string(test_input), result, test.want)
		}
	}

}

/*
Tests the Task.GetHash method, which is built to ensure tasks can be uniquly identified by their
params + children, i.e. if two task runners have the same params and the same children, they
should produce the same hash.
*/
func TestGetHash(t *testing.T) {
	test1 := createTestTaskRunner("test1", 1)
	test2 := createTestTaskRunner("test1", 1)

	test3 := createTestTaskRunner("test3", 3)
	test4 := createTestTaskRunner("test3", 3)

	test5 := createTestTaskRunner("test5", 1)
	test6 := createTestTaskRunner("test5", 1)

	test7 := createTestTaskRunner("test7", 2)
	test8 := createTestTaskRunner("test8", 3)

	test1.AddChildren(test3, test4)
	test2.AddChildren(test3, test4)

	test5.AddChildren(test7, test8)
	test6.AddChildren(test7)

	var tests = []struct {
		input []TaskRunner
		want  bool
	}{
		{[]TaskRunner{test1, test2}, true},
		{[]TaskRunner{test5, test6}, false},
		{[]TaskRunner{test7, test8}, false},
	}
	for _, test := range tests {
		task_hashes := []string{}
		for _, runner := range test.input {
			task_hashes = append(task_hashes, runner.GetTask().GetHash())
		}

		first_hash, second_hash := task_hashes[0], task_hashes[1]
		result := first_hash == second_hash

		if result != test.want {
			t.Errorf("GetHash Failed got %v not %v: hash1 %s hash2 %s", result, test.want, first_hash, second_hash)
		}
	}

}

func TestCreateTaskRunnerFromParams(t *testing.T) {
	test1 := createTestTaskRunner("test1", 1)
	// modify default value
	test1.N = 25
	task_params, _ := CreateAndSetTaskParams(test1)
	test1_clone := createTestTaskRunner("test1_clone", 1)
	// TODO: re-name this Function
	CreateTaskRunnerFromParams(test1_clone, task_params)
	if !compareTestTaskParams(test1, test1_clone) {
		t.Errorf("CreateTaskRunnerFromParams failed to clone, %v instead of %v", test1_clone, test1)
	}

}

func TestCreateParamFromSerializedParam(t *testing.T) {
	serialized_task_param1 := "Foop:INT:5"
	task_params, _ := DeserializeTaskParams(serialized_task_param1)
	new_task_param := task_params[0]
	if new_task_param.Name != "Foop" {
		t.Errorf("Failed to set correct name on TaskParam %s", "Foop")
	}
	if new_task_param.Data.Int() != 5 {
		t.Errorf("Failed to set correct value on TaskParam %d", 5)
	}

	serialized_task_param2 := "Poof:STR:HEYA"
	task_params, _ = DeserializeTaskParams(serialized_task_param2)
	new_task_param = task_params[0]
	if new_task_param.Name != "Poof" {
		t.Errorf("Failed to set correct name on TaskParam expected: %s got: %s", "Poof", new_task_param.Name)
	}
	if new_task_param.Data.String() != "HEYA" {
		t.Errorf("Failed to set correct value on TaskParam, expected: %s got:%s", "HEYA", new_task_param.Data.String())
	}

}

func TestCreateTaskRunnerFromHash(t *testing.T) {
	// TODO: add string param
	serialized_task_param1 := "N:INT:5_X:STR:HEYA"
	test_1 := createTestTaskRunner("test1", 0)
	_ = CreateAndSetTaskParamsFromHash(test_1, serialized_task_param1)
	if test_1.N != 5 {
		t.Errorf("Failed to set N on test struct %d", test_1.N)
	}
	if test_1.X != "HEYA" {
		t.Errorf("Failed to set X on test struct %s", test_1.X)
	}
	if test_1.GetSerializedParams() != serialized_task_param1 {
		t.Errorf("Serialized params dont match restored task runner params, %s -> %s", test_1.GetSerializedParams(), serialized_task_param1)

	}

}

func TestSetTaskPriorities(t *testing.T) {
	test1 := createTestTaskRunner("test1", 0)
	test2 := createTestTaskRunner("test2", 0)
	test3 := createTestTaskRunner("test3", 0)

	test1.AddChildren(test2)
	test2.AddChildren(test3)

	err := SetTaskPriorities(test1.GetTask())
	if err != nil {
		t.Error(err)
	}
	var tests = []struct {
		input            TaskRunner
		expectedPriority int
	}{
		{test1, 2},
		{test2, 1},
		{test3, 0},
	}
	for _, test := range tests {
		if test.input.GetTask().Priority != test.expectedPriority {
			t.Errorf("TestTaskRunner %s had priority %d but we expected %d", test.input.GetTask().Name, test.input.GetTask().Priority, test.expectedPriority)
		}
	}
}

type TestTaskDate struct {
	*Task
	INT_PARAM    int       `task_param:""`
	STRING_PARAM string    `task_param:""`
	DATE_PARAM1  time.Time `task_param:""`
	DATE_PARAM2  time.Time `task_param:""`
}

func (tt *TestTaskDate) Run() {
	return
}
func (tt *TestTaskDate) GetTask() *Task {
	return tt.Task
}

func createTestTaskDate(strParam string, intParam int, date1, date2 time.Time) *TestTaskDate {
	new_runner := TestTaskDate{
		INT_PARAM:    intParam,
		STRING_PARAM: strParam,
		DATE_PARAM1:  date1,
		DATE_PARAM2:  date2}
	new_runner.Task = NewTask("dateTaskRunner")
	CreateAndSetTaskParams(&new_runner)
	return &new_runner

}

func TestTaskDateTaskParam(t *testing.T) {
	timeFormat := "2006-01-02 15:04 MST"
	testTime1 := "2018-09-30 20:57 UTC"
	testTime2 := "2014-10-01 20:57 UTC"
	start, _ := time.Parse(timeFormat, testTime1)
	end, _ := time.Parse(timeFormat, testTime2)
	dateRunner := createTestTaskDate("IAMTESTPARAM", 337, start, end)
	serializedParam := dateRunner.GetSerializedParams()
	fmt.Println(serializedParam)
	spew.Dump(dateRunner.Params)
}
