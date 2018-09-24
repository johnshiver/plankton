package task

import (
	"fmt"
	"hash/fnv"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
)

type TaskRunner interface {
	// NOTE: Run() should always be called by RunTaskRunner
	Run()
	// requires TaskRuner to have an embedded Task
	GetTask() *Task
}

// TODO: add docs
type Task struct {
	Name           string
	Children       []TaskRunner
	Parent         TaskRunner
	ResultsChannel chan string
	State          string
	Params         []*TaskParam
	Start          time.Time
	End            time.Time
	DataProcessed  int
}

type TaskParam struct {
	Name string
	Data reflect.Value
}

func NewTask(name string) *Task {
	// TODO: make it possible to accept buffer size on results channel
	//       some tasks may want a buffer, others may not

	return &Task{
		Name:           name,
		Children:       []TaskRunner{},
		Parent:         nil,
		ResultsChannel: make(chan string, 10000),
		State:          "waiting",
		Params:         []*TaskParam{},
		DataProcessed:  0,
	}
}

func makeStringHash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()

}

func (ts *Task) GetHash() string {
	strings_to_hash := []string{}
	strings_to_hash = append(strings_to_hash, ts.Name)
	strings_to_hash = append(strings_to_hash, ts.GetSerializedParams())
	for _, child := range ts.Children {
		strings_to_hash = append(strings_to_hash, child.GetTask().GetSerializedParams())
	}
	string_to_hash := strings.Join(strings_to_hash, "")
	return fmt.Sprintf("%v", (makeStringHash(string_to_hash)))
}

/*
   Given a Task, return a serialized string to represent it.

   This is useful for storing the state of each Task that is run in the dag, and can
   be used to re-create a previously run task for DAG re-runs, backfills, or some other
   use case.

   Example:

    type TestTask struct {
	    *Task
	    N int    `task_param:""`
	    X string `task_param:""`
	    Z int
    }

    new_test_task := TestTask{
	5,
	"HI",
	0,
    }
    serialized_test_task := new_test_task.GetSerializedParams()
    fmt.Println(serialized_test_task)
    > N:INT:5_X:STR:HI

    This function uses a lot of reflection / is kind of tricky. Would be good to document
    exactly how this works because I constantly forget what these variables mean :P
*/

// TODO: i think this method needs a rename
func (ts *Task) GetSerializedParams() string {

	param_strings := []string{}
	for _, param := range ts.Params {
		var data_val, data_type string

		// TODO: Should support all types in SetParam
		//       maybe there is a way to combine the logic

		// TODO: add support for some Date type, that should be enough to start
		//       working on running DAGS, storing their state, then re-running them
		//       with another command
		switch param.Data.Kind() {
		case reflect.Int:
			data_val = fmt.Sprintf("%v", param.Data.Int())
			data_type = "INT"
		case reflect.String:
			data_val = param.Data.String()
			data_type = "STR"
		default:
			// TODO: think about what to do in this case
			fmt.Printf("Param %s not included in the serialized task, its type is not currently supported.\n", param.Name)
		}

		param_serializer_elements := []string{
			param.Name,
			data_type,
			data_val,
		}
		param_strings = append(param_strings, strings.Join(param_serializer_elements, ":"))
	}
	return strings.Join(param_strings, "_")

}

func DeserializeTaskParams(serialized_pr string) ([]*TaskParam, error) {
	deserialized_task_params := []*TaskParam{}
	hashed_params := strings.Split(serialized_pr, "_")
	for _, param := range hashed_params {
		split_vals := strings.Split(param, ":")
		name, d_type, d_val := split_vals[0], split_vals[1], split_vals[2]
		var final_val reflect.Value

		switch d_type {
		case "INT":
			final_int, err := strconv.Atoi(d_val)
			if err != nil {
				fmt.Errorf("Error creating int from serialized Task Param!")
				return nil, err
			}
			final_val = reflect.ValueOf(final_int)
		case "STR":
			final_val = reflect.ValueOf(d_val)
		default:
			spew.Println(final_val)
			fmt.Println("Not supported yet")
			continue
		}

		new_param := &TaskParam{
			Name: name,
			Data: final_val,
		}
		deserialized_task_params = append(deserialized_task_params, new_param)

	}
	return deserialized_task_params, nil

}

// Uses reflection to inspect struct elements for 'task_param' tag
// and sets tr.Task.Params accordingly
func CreateAndSetTaskParams(tr TaskRunner) ([]*TaskParam, error) {

	const TASK_PARAM_TAG = "task_param"
	var task_params []*TaskParam

	v := reflect.ValueOf(tr).Elem()
	for i := 0; i < v.NumField(); i++ {
		field_info := v.Type().Field(i)
		tag := field_info.Tag
		_, ok := tag.Lookup(TASK_PARAM_TAG)
		if ok {
			new_param := TaskParam{
				Name: field_info.Name,
				Data: getFieldValue(tr, field_info.Name),
			}
			task_params = append(task_params, &new_param)

		}
	}

	tr.GetTask().Params = task_params
	return tr.GetTask().Params, nil
}

func getFieldValue(tr TaskRunner, field_name string) reflect.Value {
	// TODO: check this works on different types other than int
	tr_reflect := reflect.ValueOf(tr)
	field_val := reflect.Indirect(tr_reflect).FieldByName(field_name)
	return field_val

}

// Given Params and a TaskRunner, sets all TaskRunner fields marked as param
// Note: The struct satisfying the TaskRunner interface MUST be passed to this function
// as a reference.  See these articles for a more thorough explanation:
// https://stackoverflow.com/questions/6395076/using-reflect-how-do-you-set-the-value-of-a-struct-field
// http://speakmy.name/2014/09/14/modifying-interfaced-go-struct/
func CreateTaskRunnerFromParams(tr TaskRunner, params []*TaskParam) error {

	// TODO: change name of this Function.  It doesnt really create a task runner so much
	//       as fill in param values on an existing TaskRunner

	// TODO: make this var name consistent with the one used above
	stype := reflect.ValueOf(tr).Elem()

	param_name_value_map := map[string]reflect.Value{}
	for _, param := range params {
		param_name_value_map[param.Name] = param.Data
	}

	if stype.Kind() == reflect.Struct {
		for name, val := range param_name_value_map {
			f := stype.FieldByName(name)
			if f.CanSet() {
				// TODO: support more kinds of fields
				switch f.Kind() {
				case reflect.Int:
					f.SetInt(val.Int())
				case reflect.String:
					f.SetString(val.String())
				default:
					// TODO: think about what to do in this case
					return fmt.Errorf("%s not supported as TaskParam yet!", f.Kind())
				}

			} else {
				fmt.Printf("Cannot set %s %v\n", name, f)
			}
		}
	}

	return nil

}

func CreateAndSetTaskParamsFromHash(tr TaskRunner, param_hash string) error {
	if tr.GetTask() == nil {
		return fmt.Errorf("tr %v didnt have task set", tr)
	}

	task_params, err := DeserializeTaskParams(param_hash)
	if err != nil {
		return err
	}

	err = CreateTaskRunnerFromParams(tr, task_params)
	if err != nil {
		return err
	}

	CreateAndSetTaskParams(tr)
	return nil

}

func (ts *Task) AddChildren(children ...TaskRunner) []TaskRunner {
	new_task_children := []TaskRunner{}
	for _, child := range children {
		new_task_children = append(new_task_children, child)
	}

	ts.Children = new_task_children
	return ts.Children
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
		return "", fmt.Errorf("Invalid state on task %s", new_state)
	}

	ts.State = new_state
	return ts.State, nil
}

// Runs a TaskRunner, sets state and notifies waiting group when run is done
func RunTaskRunner(tsk_runner TaskRunner, wg *sync.WaitGroup) {
	// TODO: add that failsafe i read in rob fig's cron project
	defer wg.Done()
	tsk_runner.GetTask().SetState("running")
	fmt.Printf("Running Task: %s\n", tsk_runner.GetTask().Name)

	runner_children := tsk_runner.GetTask().Children
	if len(runner_children) > 0 {
		for _, child := range runner_children {
			child.GetTask().Parent = tsk_runner
		}

		parent_wg := &sync.WaitGroup{}
		for _, child := range runner_children {
			parent_wg.Add(1)
			go RunTaskRunner(child, parent_wg)
		}

		go func() {
			parent_wg.Wait()
			close(tsk_runner.GetTask().ResultsChannel)
		}()
	}
	tsk_runner.GetTask().Start = time.Now()
	tsk_runner.Run()
	tsk_runner.GetTask().SetState("complete")
	tsk_runner.GetTask().End = time.Now()
}

// TODO: do a better job detecting the D part
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
