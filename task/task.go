package task

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/phf/go-queue/queue"
)

const (
	WAITING  = "waiting"
	RUNNING  = "running"
	COMPLETE = "complete"
)

type Task struct {
	Name         string
	Children     []*Task
	Parent       *Task
	WorkerTokens chan struct{} `json:"-"`
	State        string
	Priority     int
	Start        time.Time
	End          time.Time

	Logger *log.Logger
	mux    sync.Mutex

	// might be important that the dag can actually re-hydate
	// TODO: figure out what to do with this JSON field / determine its utility
	Runner Runner `json:"Runner"`
}

// NewTask ...
//
func NewTask(name string, runner Runner) *Task {
	return &Task{
		Name:   name,
		Runner: runner,
	}
}

func (ts *Task) Hash() (string, error) {
	raw, err := json.Marshal(ts)
	if err != nil {
		return "", err
	}

	h := md5.New()
	_, err = h.Write(raw)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

// SetState ...
//
func (ts *Task) SetState(newState string) (string, error) {
	ts.mux.Lock()
	defer ts.mux.Unlock()

	validStates := []string{
		WAITING, RUNNING, COMPLETE,
	}

	ValidStateParam := false
	for _, state := range validStates {
		if newState == state {
			ValidStateParam = true
			break
		}
	}

	if !ValidStateParam {
		return "", fmt.Errorf("invalid state on task %s", newState)
	}

	ts.State = newState
	return ts.State, nil
}

// SetTaskPriorities ...
//
func SetTaskPriorities(root *Task) error {
	/*
	   Runs DFS on root of task DAG to set task priorities order of precedence
	   Assumes root is a valid dag.
	*/
	goodDag, err := VerifyDAG(root)
	if err != nil {
		return fmt.Errorf("while verifying dag: %w", err)
	}
	if !goodDag {
		return fmt.Errorf("root task %s isnt a valid Task DAG", root.Name)
	}

	currPriority := 0
	var setTaskPriorities func(root *Task)
	setTaskPriorities = func(root *Task) {
		for _, child := range root.Children {
			setTaskPriorities(child)
		}
		root.Priority = currPriority
		currPriority++
	}

	setTaskPriorities(root)
	return nil
}

func VerifyDAG(rootTask *Task) (bool, error) {
	taskSet := make(map[*Task]struct{})

	var taskQ []*Task
	taskQ = append(taskQ, rootTask)
	for len(taskQ) > 0 {
		// if we've seen a task, there is a cycle
		curr := taskQ[0]
		taskQ = taskQ[1:]

		_, ok := taskSet[curr]
		if ok {
			return false, nil
		}
		taskSet[curr] = struct{}{}
		taskQ = append(taskQ, curr.Children...)
	}
	return true, nil
}

func (ts *Task) AddChildren(children []*Task) {
	ts.mux.Lock()
	defer ts.mux.Unlock()
	ts.Children = append(ts.Children, children...)
}

func (ts *Task) SetParentOnChildren() {
	for _, child := range ts.Children {
		child.Parent = ts
		child.SetParentOnChildren()
	}
}

// RunTaskRunner ...
// Runs a TaskRunner, sets state and notifies waiting group when run is done
func RunTask(ctx context.Context, task *Task, wg *sync.WaitGroup, TokenReturn chan struct{}, Errors chan error) {
	defer wg.Done()

	// Start children in goroutines
	children := task.Children
	if len(children) > 0 {
		var parentWG sync.WaitGroup
		for _, child := range children {
			parentWG.Add(1)
			go RunTask(ctx, child, &parentWG, TokenReturn, Errors)
		}
		go func() {
			parentWG.Wait()
		}()
	}

	var (
		token struct{}
		done  = false
	)
	for !done {
		select {
		case <-ctx.Done():
			task.Logger.Printf("%s canceled by context", task.Name)
			done = true
		case token = <-task.WorkerTokens:
			task.Logger.Printf("Starting %s", task.Name)
			task.Start = time.Now().UTC()
			_, _ = task.SetState(RUNNING)
			err := task.Runner.Run(ctx)
			if err != nil {
				Errors <- err
			}
			_, _ = task.SetState(COMPLETE)
			task.End = time.Now().UTC()
			task.Logger.Printf("%s finished", task.Name)
			TokenReturn <- token
			done = true
		}
	}
}

// ClearDAGState ...
//
func ClearDAGState(root *Task) {
	runnerQ := queue.New()
	runnerQ.PushBack(root)
	for runnerQ.Len() > 0 {
		curr := runnerQ.PopFront().(*Task)
		curr.Start = time.Time{}
		curr.End = time.Time{}
		for _, child := range curr.Children {
			runnerQ.PushBack(child)
		}
	}
}
