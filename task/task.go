package task

import (
	"fmt"
	"hash/fnv"
	"reflect"
	"strings"
	"sync"
	"time"
)

type TaskRunner interface {
	// NOTE: Run() should always be called by RunTaskRunner
	Run()
	// requires TaskRuner to have an embedded Task
	GetTask() *Task
	// GetCompletionPercent() int
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
		Children:       nil,
		Parent:         nil,
		ResultsChannel: make(chan string, 1000),
		State:          "waiting",
		Params:         []*TaskParam{},
	}
}

func makeStringHash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()

}

// TODO: Consider getting rid of this
//func (ts *Task) AddChild(child TaskRunner) []TaskRunner {
//	ts.Children = append(ts.Children, child)
//	return ts.Children
//}

func (ts *Task) GetHash() string {
	param_string := GetParamsHashString(ts.Params)
	param_hash := makeStringHash(param_string)
	hash_elements := []string{
		ts.Name,
		param_string,
		fmt.Sprintf("%v", param_hash),
	}
	return strings.Join(hash_elements, "_")
}

func GetParamsHashString(params []*TaskParam) string {
	param_strings := []string{}
	for _, param := range params {
		var data_val, data_type string

		// TODO: Should support all types in SetParam
		//       maybe there is a way to combine the logic
		switch param.Data.Kind() {
		case reflect.Int:
			data_val = fmt.Sprintf("%v", param.Data.Int())
			data_type = "INT"
		case reflect.String:
			data_val = param.Data.String()
			data_type = "STR"
		default:
			// TODO: think about what to do in this case
			fmt.Println("Param not supported in hashing")
		}

		data_hash_elems := []string{
			param.Name,
			data_val,
			data_type,
		}
		param_strings = append(param_strings, strings.Join(data_hash_elems, ":"))
	}
	return strings.Join(param_strings, "_")

}

func GetParamsFromHashString(params_hash string) []*TaskParam {
	hashed_params := strings.Split(params_hash, "_")
	hashed_params = hashed_params[1 : len(hashed_params)-1]
	fmt.Println(hashed_params)
	return []*TaskParam{}

}

// TODO: rename this function or maybe change the API for setting task params
//       on a TaskRunner

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
		return "", fmt.Errorf("Invalid state on task {}", new_state)
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

		fmt.Println(curr.GetHash())
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
