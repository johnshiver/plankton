package task

import (
	"testing"
)

type TestTask struct {
	task *Task
}

func (tt *TestTask) Run() {
	return
}

func (tt *TestTask) GetTask() *Task {
	return tt.task
}

func createTestTaskRunner(name string) TaskRunner {
	task := NewTask(
		name,
		[]TaskRunner{},
		nil,
	)
	return &TestTask{
		task: task,
	}

}

func TestVerifyDAG(t *testing.T) {

	good_dag := createTestTaskRunner("good_dag")
	test2 := createTestTaskRunner("test2")
	test3 := createTestTaskRunner("test3")

	good_dag.GetTask().Children = []TaskRunner{
		test2,
		test3,
	}

	bad_dag := createTestTaskRunner("bad_dag")
	test4 := createTestTaskRunner("test4")
	test5 := createTestTaskRunner("test5")

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
