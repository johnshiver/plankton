package task

import (
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/johnshiver/plankton/config"
	"github.com/phf/go-queue/queue"
)

// TaskRunner ...
//
type TaskRunner interface {
	// NOTE: Run() should always be called by RunTaskRunner

	// TODO: have Run() return an error, which could be given to scheduler
	Run()

	// requires TaskRuner to have an embedded Task
	GetTask() *Task
}

// CreateAndSetTaskParams ...
// Uses reflection to inspect struct elements for 'task_param' tag
// and sets tr.Task.Params accordingly
func CreateAndSetTaskParams(tr TaskRunner) ([]*TaskParam, error) {

	const TASK_PARAM_TAG = "task_param"
	var taskParams []*TaskParam

	v := reflect.ValueOf(tr).Elem()
	for i := 0; i < v.NumField(); i++ {
		fieldInfo := v.Type().Field(i)
		tag := fieldInfo.Tag
		_, ok := tag.Lookup(TASK_PARAM_TAG)
		if ok {
			newParam := TaskParam{
				Name: fieldInfo.Name,
				Data: getFieldValue(tr, fieldInfo.Name),
			}
			taskParams = append(taskParams, &newParam)

		}
	}

	tr.GetTask().Params = taskParams
	return tr.GetTask().Params, nil
}

func getFieldValue(tr TaskRunner, fieldName string) reflect.Value {
	// TODO: check this works on different types other than int
	trReflect := reflect.ValueOf(tr)
	fieldVal := reflect.Indirect(trReflect).FieldByName(fieldName)
	return fieldVal
}

// CreateTaskRunnerFromParams ...
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

	paramNameValueMap := map[string]reflect.Value{}
	for _, param := range params {
		paramNameValueMap[param.Name] = param.Data
	}

	if stype.Kind() == reflect.Struct {
		for name, val := range paramNameValueMap {
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
					return fmt.Errorf("%s not supported as TaskParam yet", f.Kind())
				}

			} else {
				tr.GetTask().Logger.Printf("Cannot set %s %v\n", name, f)
			}
		}
	}

	return nil

}

// CreateAndSetTaskParamsFromHash ....
//
func CreateAndSetTaskParamsFromHash(tr TaskRunner, paramHash string) error {
	if tr.GetTask() == nil {
		return fmt.Errorf("tr %v didnt have task set", tr)
	}

	taskParams, err := DeserializeTaskParams(paramHash)
	if err != nil {
		return err
	}

	err = CreateTaskRunnerFromParams(tr, taskParams)
	if err != nil {
		return err
	}

	CreateAndSetTaskParams(tr)
	return nil

}

// AddChildren ...
//
func (ts *Task) AddChildren(children ...TaskRunner) []TaskRunner {
	ts.mux.Lock()
	defer ts.mux.Unlock()

	var tChildren []TaskRunner
	if len(ts.Children) < 1 {
		tChildren = []TaskRunner{}

	} else {
		tChildren = ts.Children
	}
	for _, child := range children {
		tChildren = append(tChildren, child)
	}

	ts.Children = tChildren
	return ts.Children
}

func SetParents(tRunner TaskRunner) {
	for _, child := range tRunner.GetTask().Children {
		child.GetTask().Parent = tRunner
		SetParents(child)
	}
}

// RunTaskRunner ...
// Runs a TaskRunner, sets state and notifies waiting group when run is done
func RunTaskRunner(tRunner TaskRunner, wg *sync.WaitGroup, TokenReturn chan struct{}) {
	// TODO: add that failsafe i read in rob fig's cron project
	defer wg.Done()

	runnerChildren := tRunner.GetTask().Children
	if len(runnerChildren) > 0 {

		parentWG := &sync.WaitGroup{}
		for _, child := range runnerChildren {
			parentWG.Add(1)
			go RunTaskRunner(child, parentWG, TokenReturn)
		}

		go func() {
			parentWG.Wait()
			close(tRunner.GetTask().ResultsChannel)
		}()
	}

	done := false
	var token struct{}
	for !done {
		select {
		case token = <-tRunner.GetTask().WorkerTokens:
			tRunner.GetTask().Logger.Printf("Starting task")
			tRunner.GetTask().SetStart(time.Now())
			tRunner.GetTask().SetState(RUNNING)
			tRunner.Run()
			tRunner.GetTask().SetState(COMPLETE)
			tRunner.GetTask().SetEnd(time.Now())
			tRunner.GetTask().Logger.Printf("Task finished")
			TokenReturn <- token
			done = true
		}
	}
}

// ResetDAGResultChannels ...
// Need to recreate result channels after each scheduler run, because they are close
// in RunTaskRunner
func ResetDAGResultChannels(RootRunner TaskRunner) {
	c := config.GetConfig()
	runnerQ := queue.New()
	runnerQ.PushBack(RootRunner)
	for runnerQ.Len() > 0 {
		curr := runnerQ.PopFront().(TaskRunner)
		curr.GetTask().ResultsChannel = make(chan string, c.ResultChannelSize)
		for _, child := range curr.GetTask().Children {
			runnerQ.PushBack(child)
		}
	}
}

// ClearDAGState ...
//
func ClearDAGState(RootRunner TaskRunner) {
	runnerQ := queue.New()
	runnerQ.PushBack(RootRunner)
	for runnerQ.Len() > 0 {
		curr := runnerQ.PopFront().(TaskRunner)
		curr.GetTask().DataProcessed = 0
		curr.GetTask().SetStart(time.Time{})
		curr.GetTask().SetEnd(time.Time{})

		for _, child := range curr.GetTask().Children {
			runnerQ.PushBack(child)
		}
	}
}
