package task

import (
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
)

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
	newRunner := TestTask{
		NewTask(name),
		n,
		"TestString",
		0,
	}
	CreateAndSetTaskParams(&newRunner)
	return &newRunner

}

func TestVerifyDAG(t *testing.T) {
	// TODO: add more variations of bad dags

	goodDag := createTestTaskRunner("goodDag", 1)
	test2 := createTestTaskRunner("test2", 1)
	test3 := createTestTaskRunner("test3", 1)

	goodDag.GetTask().Children = []TaskRunner{
		test2,
		test3,
	}

	badDag := createTestTaskRunner("badDag", 1)
	test4 := createTestTaskRunner("test4", 1)
	test5 := createTestTaskRunner("test5", 1)

	badDag.GetTask().Children = []TaskRunner{
		test4,
		test5,
	}

	test4.GetTask().Children = []TaskRunner{
		badDag,
	}

	var tests = []struct {
		input *Task
		want  bool
	}{
		{goodDag.GetTask(), true},
		{badDag.GetTask(), false},
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
		serializedTaskParams := []string{}
		for _, runner := range test.input {
			CreateAndSetTaskParams(runner)
			serializedTaskParams = append(serializedTaskParams, runner.GetTask().GetSerializedParams())
		}

		sParam1, sParam2 := serializedTaskParams[0], serializedTaskParams[1]
		result := sParam1 == sParam2

		if result != test.want {
			testInput := spew.Sdump(test.input)
			t.Errorf("GetSerializedParams Failed %s got %v not %v", string(testInput), result, test.want)
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
		taskHashes := []string{}
		for _, runner := range test.input {
			taskHashes = append(taskHashes, runner.GetTask().GetHash())
		}

		firstHash, secondHash := taskHashes[0], taskHashes[1]
		result := firstHash == secondHash

		if result != test.want {
			t.Errorf("GetHash Failed got %v not %v: hash1 %s hash2 %s", result, test.want, firstHash, secondHash)
		}
	}

}

func TestCreateTaskRunnerFromParams(t *testing.T) {
	test1 := createTestTaskRunner("test1", 1)
	// modify default value
	test1.N = 25
	taskParams, _ := CreateAndSetTaskParams(test1)
	test1Clone := createTestTaskRunner("test1Clone", 1)
	// TODO: re-name this Function
	CreateTaskRunnerFromParams(test1Clone, taskParams)
	if !compareTestTaskParams(test1, test1Clone) {
		t.Errorf("CreateTaskRunnerFromParams failed to clone, %v instead of %v", test1Clone, test1)
	}

}

func TestCreateParamFromSerializedParam(t *testing.T) {
	serializedTaskParam1 := "Foop:INT:5"
	taskParams, _ := DeserializeTaskParams(serializedTaskParam1)
	newTaskParam := taskParams[0]
	if newTaskParam.Name != "Foop" {
		t.Errorf("Failed to set correct name on TaskParam %s", "Foop")
	}
	if newTaskParam.Data.Int() != 5 {
		t.Errorf("Failed to set correct value on TaskParam %d", 5)
	}

	serializedTaskParam2 := "Poof:STR:HEYA"
	taskParams, _ = DeserializeTaskParams(serializedTaskParam2)
	newTaskParam = taskParams[0]
	if newTaskParam.Name != "Poof" {
		t.Errorf("Failed to set correct name on TaskParam expected: %s got: %s", "Poof", newTaskParam.Name)
	}
	if newTaskParam.Data.String() != "HEYA" {
		t.Errorf("Failed to set correct value on TaskParam, expected: %s got:%s", "HEYA", newTaskParam.Data.String())
	}

}

func TestCreateTaskRunnerFromHash(t *testing.T) {
	// TODO: add string param
	serializedTaskParam1 := "N:INT:5_X:STR:HEYA"
	test1 := createTestTaskRunner("test1", 0)
	_ = CreateAndSetTaskParamsFromHash(test1, serializedTaskParam1)
	if test1.N != 5 {
		t.Errorf("Failed to set N on test struct %d", test1.N)
	}
	if test1.X != "HEYA" {
		t.Errorf("Failed to set X on test struct %s", test1.X)
	}
	if test1.GetSerializedParams() != serializedTaskParam1 {
		t.Errorf("Serialized params dont match restored task runner params, %s -> %s", test1.GetSerializedParams(), serializedTaskParam1)

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

func TestSetParents(t *testing.T) {
	test1 := createTestTaskRunner("test1", 0)
	test2 := createTestTaskRunner("test2", 0)
	test3 := createTestTaskRunner("test3", 0)
	test1.AddChildren(test2)
	test2.AddChildren(test3)
	SetParents(test1)

	var tests = []struct {
		input          TaskRunner
		expectedParent TaskRunner
	}{
		{test1, nil},
		{test2, test1},
		{test3, test2},
	}

	for _, test := range tests {
		if test.input.GetTask().Parent != test.expectedParent {
			t.Errorf("TaskRunner %s didnt have expected parent %s", test.input.GetTask().Name, test.expectedParent)
		}

	}

}

func TestSetRanges(t *testing.T) {
	test1 := createTestTaskRunner("test1", 0)
	test2 := createTestTaskRunner("test2", 0)
	test3 := createTestTaskRunner("test3", 0)
	test1.AddChildren(test2)
	test2.AddChildren(test3)
	rangeStart := "2019-01-02T00:00:00Z"
	rangeEnd := "2019-01-03T00:00:00Z"
	SetParents(test1)
	SetTaskRanges(test1, rangeStart, rangeEnd)

	var tests = []struct {
		input              TaskRunner
		expectedRangeStart string
		expectedRangeEnd   string
	}{
		{test1, rangeStart, rangeEnd},
		{test2, rangeStart, rangeEnd},
		{test3, rangeStart, rangeEnd},
	}

	for _, test := range tests {
		if test.input.GetTask().RangeStart != test.expectedRangeStart {
			t.Errorf("TaskRunner %s had RangeStart %s, wanted %s", test.input.GetTask().Name, test.input.GetTask().RangeStart, test.expectedRangeStart)
		}
		if test.input.GetTask().RangeEnd != test.expectedRangeEnd {
			t.Errorf("TaskRunner %s had RangeEnd %s, wanted %s", test.input.GetTask().Name, test.input.GetTask().RangeEnd, test.expectedRangeEnd)
		}

	}

}
