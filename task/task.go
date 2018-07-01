package task

import (
	"fmt"
	"hash/fnv"
	"reflect"
	"strings"
	"sync"
)

type TaskRunner interface {
	Run()
	// requires TaskRuner to have an embedded Task
	GetTask() *Task
}

type Task struct {
	// TODO: does children need to be TaskRunner or can I get away with making everything
	//       a task
	Name           string
	Children       []TaskRunner
	Parent         TaskRunner
	ResultsChannel chan string
	State          string
	Params         []*TaskParam
}

type TaskParam struct {
	Name  string
	Value string
}

func NewTask(name string, children []TaskRunner, parent TaskRunner) *Task {
	return &Task{
		Name:           name,
		Children:       children,
		Parent:         parent,
		ResultsChannel: make(chan string),
		State:          "waiting",
		Params:         []*TaskParam{},
	}
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()

}

func (ts *Task) AddChild(child TaskRunner) []TaskRunner {
	ts.Children = append(ts.Children, child)
	return ts.Children
}

func (ts *Task) GetHash() string {
	params_to_string := []string{}
	for _, parm := range ts.Params {
		params_to_string = append(params_to_string, fmt.Sprintf("%s:%s", parm.Name, parm.Value))
	}
	param_string := strings.Join(params_to_string, "_")
	param_hash := hash(param_string)
	return fmt.Sprintf("{}_{}_{}", ts.Name, param_string, param_hash)
}

// Uses reflection to inspect struct elements for 'task_param' tag
// and sets tr.Task.Params accordingly
func SetTaskParams(tr TaskRunner) ([]*TaskParam, error) {

	const TASK_PARAM_TAG = "task_param"
	var task_params []*TaskParam

	v := reflect.ValueOf(tr).Elem()
	for i := 0; i < v.NumField(); i++ {
		field_info := v.Type().Field(i)
		tag := field_info.Tag
		_, ok := tag.Lookup(TASK_PARAM_TAG)
		if ok {
			new_param := TaskParam{
				Name:  strings.ToLower(field_info.Name),
				Value: getField(tr, field_info.Name),
			}
			task_params = append(task_params, &new_param)
		}

	}

	tr.GetTask().Params = task_params
	return tr.GetTask().Params, nil
}

func getField(tr TaskRunner, field_name string) string {
	// TODO: check this works on different types other than int
	tr_reflect := reflect.ValueOf(tr)
	field_val := reflect.Indirect(tr_reflect).FieldByName(field_name)
	return fmt.Sprintf("%v", field_val)

}

func (ts *Task) SetState(new_state string) (string, error) {
	valid_states := []string{
		"waiting",
		"running",
		"complete",
	}

	valid_state_param := false
	for _, state := range valid_states {
		if new_state == state {
			valid_state_param = true
			break
		}

	}

	if !valid_state_param {
		return "", fmt.Errorf("Invalid state on task {}", new_state)
	}

	ts.State = new_state
	return ts.State, nil
}

func RunTask(tsk_runner TaskRunner, wg *sync.WaitGroup) {
	defer wg.Done()
	tsk_runner.GetTask().SetState("running")
	fmt.Printf("Running Task: %s\n", tsk_runner.GetTask().Name)
	tsk_runner.Run()
	tsk_runner.GetTask().SetState("complete")
}

// TODO: do a better job detecting the D part

// TODO: we also need some way of creating a map of tasks
//       to check acyclical property
//       Going to implement task_has on Task
func VerifyDAG(root_task *Task) bool {

	task_set := make(map[string]struct{})

	task_queue := []*Task{}
	task_queue = append(task_queue, root_task)
	for len(task_queue) > 0 {
		curr := task_queue[0]
		task_queue = task_queue[1:]

		_, ok := task_set[curr.GetHash()]
		if ok {
			return false
		} else {
			task_set[curr.GetHash()] = struct{}{}
		}
		for _, child := range curr.Children {
			task_queue = append(task_queue, child.GetTask())
		}

	}

	return true
}
