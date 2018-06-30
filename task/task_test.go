package task

import (
	"reflect"
	"testing"
)

type TestTask struct {
	n    int `task_param:""`
	z    int
	task *Task
}

func (tt *TestTask) Run() {
	return
}

func (tt *TestTask) GetTask() *Task {
	return tt.task
}

func createTestTaskRunner(name string, n int) TaskRunner {
	task := NewTask(
		name,
		[]TaskRunner{},
		nil,
	)
	return &TestTask{
		task: task,
		n:    n,
	}

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
				Name:  "n",
				Value: "1",
			},
		},
		},
	}

	for _, test := range tests {
		if output := SetTaskParams(test.input); !(reflect.DeepEqual(output, test.want)) {
			t.Errorf("SetTaskParams(%v) = %v", test.input, output)
		}
	}

}

//func TestGetHash(t *testing.T) {
//	test1 := createTestTaskRunner("test1", 1)
//	test2 := createTestTaskRunner("test1", 1)
//
//	test3 := createTestTaskRunner("test3", 2)
//	test4 := createTestTaskRunner("test3", 3)
//
//	test5 := createTestTaskRunner("test5", 1)
//	test6 := createTestTaskRunner("test6", 1)
//
//	test7 := createTestTaskRunner("test7", 2)
//	test8 := createTestTaskRunner("test8", 3)
//
//	var tests = []struct {
//		input []TaskRunner
//		want  bool
//	}{
//		{[]TaskRunner{test1, test2}, true},
//		{[]TaskRunner{test3, test4}, false},
//		{[]TaskRunner{test5, test6}, false},
//		{[]TaskRunner{test7, test8}, false},
//	}
//	for _, test := range tests {
//		task_hashes := []string{}
//		for _, runner := range test.input {
//			SetTaskParams(runner)
//			task_hashes = append(task_hashes, runner.GetTask().GetHash())
//		}
//
//		first_hash := task_hashes[0]
//		result := true
//		for _, hash := range task_hashes[1:] {
//			if result == false {
//				break
//			}
//			result = hash == first_hash
//		}
//
//		if output := VerifyDAG(test.input); output != test.want {
//			t.Errorf("VerifyDAG(%q) = %v", test.input, output)
//		}
//	}
//
//}
