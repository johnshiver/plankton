package task

import (
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
)

// TODO: make task param flag nicer
type TestTask struct {
	*Task
	N int `task_param:""`
	Z int
}

func (tt *TestTask) Run() {
	return
}
func (tt *TestTask) GetTask() *Task {
	return tt.Task
}

func compareTestTaskParams(a, b *TestTask) bool {
	if a.N == b.N {
		return true
	}
	return false
}

func createTestTaskRunner(name string, n int) *TestTask {
	new_runner := TestTask{
		NewTask(name),
		n,
		0,
	}
	SetTaskParams(&new_runner)
	return &new_runner

}

func TestVerifyDAG(t *testing.T) {

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
		if output := VerifyDAG(test.input); output != test.want {
			t.Errorf("VerifyDAG(%q) = %v", test.input, output)
		}
	}

}

func TestSetTaskParams(t *testing.T) {
	test1 := createTestTaskRunner("test1", 1)

	var tests = []struct {
		input TaskRunner
		want  []*TaskParam
	}{
		{test1, []*TaskParam{
			&TaskParam{
				Name: "N",
				Data: getFieldValue(test1, "N"),
			},
		},
		},
	}

	for _, test := range tests {
		if output, _ := SetTaskParams(test.input); !(reflect.DeepEqual(output, test.want)) {
			t.Errorf("SetTaskParams(%v) = %v, wanted: ", test.input, output, test.want)
		}
	}

}

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
		{[]TaskRunner{test5, test6}, false},
		{[]TaskRunner{test7, test8}, false},
	}
	for _, test := range tests {
		task_hashes := []string{}
		for _, runner := range test.input {
			SetTaskParams(runner)
			task_hashes = append(task_hashes, runner.GetTask().GetSerializedParams())
		}

		first_hash, second_hash := task_hashes[0], task_hashes[1]
		result := first_hash == second_hash

		if result != test.want {
			test_input := spew.Sdump(test.input)
			t.Errorf("GetSerializedParams Failed %s got %v not %v", string(test_input), result, test.want)
		}
	}

}

// Tests that task runners with identical params + name but different children produce different hashes
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
	task_params, _ := SetTaskParams(test1)
	test1_clone := createTestTaskRunner("test1_clone", 1)
	// TODO: re-name this Function
	CreateTaskRunnerFromParams(test1_clone, task_params)
	if !compareTestTaskParams(test1, test1_clone) {
		t.Errorf("CreateTaskRunnerFromParams failed to clone, %v instead of %v", test1_clone, test1)
	}

}

// Create Params From Hash

// Create TaskRunner From Hash
