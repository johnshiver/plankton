package task

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type TestTaskRunner struct {
	N    int
	X    string
	Z    int
	Data map[string]int `json:"-"`
}

func (tt *TestTaskRunner) SetStart(_ time.Time) {
}

func (tt *TestTaskRunner) SetEnd(_ time.Time) {
}

func (tt *TestTaskRunner) Run(_ context.Context) error {
	if tt.Data == nil {
		return fmt.Errorf("tt.Data is not initialized")
	}
	tt.Data[tt.X] = tt.N + tt.Z
	return nil
}

func TestVerifyDAG(t *testing.T) {
	t1 := TestTaskRunner{
		N: 0,
		X: "",
		Z: 0,
	}
	goodDag := &Task{Name: "good_dag", Runner: &t1}
	t2 := &Task{Name: "t2", Runner: &t1}
	t3 := &Task{Name: "t3", Runner: &t1}
	goodDag.Children = []*Task{
		t2, t3,
	}

	badDag := &Task{Name: "bad_dag", Runner: &t1}
	b2 := &Task{Name: "b2", Runner: &t1}
	b3 := &Task{Name: "b3", Runner: &t1}
	badDag.Children = []*Task{
		b2, b3,
	}
	b2.Children = []*Task{
		badDag,
	}

	var tests = []struct {
		input *Task
		want  bool
	}{
		{goodDag, true},
		{badDag, false},
	}
	for _, test := range tests {
		output, err := VerifyDAG(test.input)
		require.Nil(t, err)
		require.Equal(t, test.want, output)
	}
}

func TestTaskSerialization(t *testing.T) {
	t1 := TestTaskRunner{
		N: 0,
		X: "",
		Z: 0,
	}
	testTask := &Task{Name: "good_dag", Runner: &t1}
	bytes, err := json.Marshal(testTask)
	require.Nil(t, err)
	require.Equal(t, []byte(`{"Name":"good_dag","Children":null,"Parent":null,"State":"","Priority":0,"Start":"0001-01-01T00:00:00Z","End":"0001-01-01T00:00:00Z","Logger":null,"Runner":{"N":0,"X":"","Z":0}}`), bytes)
}

func TestCreateDAGFromJSON(t *testing.T) {
	t1 := TestTaskRunner{
		N: 1,
		X: "1",
		Z: 1,
	}
	t2 := TestTaskRunner{
		N: 2,
		X: "2",
		Z: 2,
	}
	t3 := TestTaskRunner{
		N: 3,
		X: "3",
		Z: 3,
	}
	goodDag := &Task{Name: "good_dag", Runner: &t1}
	child1 := &Task{Name: "t2", Runner: &t2}
	child2 := &Task{Name: "t3", Runner: &t3}
	goodDag.Children = []*Task{
		child1, child2,
	}
	bytes, err := json.Marshal(goodDag)
	require.Nil(t, err)

	var (
		t4 = TestTaskRunner{}
		t5 = TestTaskRunner{}
		t6 = TestTaskRunner{}
	)
	newDag := &Task{
		Name:   "new_dag",
		Runner: &t4,
		Children: []*Task{
			{Name: "t2", Runner: &t5},
			{Name: "t3", Runner: &t6},
		},
	}
	err = json.Unmarshal(bytes, newDag)
	require.Nil(t, err)

}

func TestSetTaskPriorities(t *testing.T) {
	t1 := TestTaskRunner{
		N: 1,
		X: "1",
		Z: 1,
	}
	newDag := Task{
		Name:   "new_dag",
		Runner: &t1,
		Children: []*Task{
			{Name: "t2", Runner: &t1},
			{Name: "t3", Runner: &t1},
		},
	}

	err := SetTaskPriorities(&newDag)
	require.Nil(t, err)
	var tests = []struct {
		input            *Task
		expectedPriority int
	}{
		{&newDag, 2},
		{newDag.Children[1], 1},
		{newDag.Children[0], 0},
	}
	for _, test := range tests {
		require.Equal(t, test.expectedPriority, test.input.Priority)
	}
}

func TestSetParents(t *testing.T) {

	t1 := TestTaskRunner{
		N: 1,
		X: "1",
		Z: 1,
	}
	newDag := Task{Name: "new_dag", Runner: &t1}
	task1 := Task{Name: "1", Runner: &t1}
	task2 := Task{Name: "2", Runner: &t1}
	task3 := Task{Name: "3", Runner: &t1}
	newDag.Children = []*Task{
		&task1, &task2,
	}
	task2.Children = []*Task{
		&task3,
	}

	newDag.SetParentOnChildren()

	var tests = []struct {
		input          *Task
		expectedParent *Task
	}{
		{&newDag, nil},
		{&task1, &newDag},
		{&task2, &newDag},
		{&task3, &task2},
	}

	for _, test := range tests {
		require.Equal(t, test.expectedParent, test.input.Parent)
	}

}
