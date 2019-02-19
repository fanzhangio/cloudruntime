package runtime

import (
	"fmt"
	"time"
)

const (
	fetchInterval = 500 * time.Millisecond
)

type localWorker struct {
	dispatcher *Dispatcher
	strategy   WorkerStrategy
}

type localContext struct {
	worker *localWorker
	handle TaskHandle
	stopCh StopChan
}

func (l *localContext) dispatcher() *Dispatcher {
	return l.worker.dispatcher
}

func (l *localContext) taskHandle() TaskHandle {
	return l.handle
}

func (l *localContext) strategy() Strategy {
	return l.dispatcher().Strategy
}

func (w *localWorker) Run(stopCh StopChan) {
	for {
		timeCh := time.After(fetchInterval)
		handle, err := w.strategy.FetchTask()
		if err == nil && handle != nil {
			w.runTaskByHandle(handle, stopCh)
			handle.Done()
		}
		select {
		case <-timeCh:
		case <-stopCh:
			return
		}
	}
}

func (w *localWorker) runTaskByHandle(handle TaskHandle, stopCh StopChan) {
	ctx := Context{
		local: &localContext{
			worker: w,
			handle: handle,
			stopCh: stopCh,
		},
	}

	err := w.runTask(ctx)
	if err != nil {
		taskErr, ok := err.(*TaskError)
		if !ok {
			taskErr = ctx.Fail(err)
		}
		err = w.taskComplete(ctx, taskErr)
	} else {
		err = w.taskComplete(ctx, nil)
	}
	if err != nil {
		// TODO
	}
}

func (w *localWorker) runTask(ctx Context) error {
	task := ctx.Task()
	stage := w.dispatcher.findStage(task.Name, task.ResumeTo)
	if stage == nil {
		return fmt.Errorf("invalid task/stage: %s/%s", task.Name, task.ResumeTo)
	}

	task.State = TaskRunning
	task.Result = TaskUnknown
	task.Stage = task.ResumeTo
	task.ResumeTo = ""
	if err := ctx.local.taskHandle().Update(&task); err != nil {
		return err
	}
	if stage.Fn == nil {
		return nil
	}

	return stage.Fn(ctx)
}

func setFailureState(task *Task, cause error) {
	if task.Revert {
		if cause != ErrTaskNonRevertable {
			task.State = TaskStucked
		}
		// else keep state as Completed
	} else {
		task.Revert = true
		task.State = TaskPending
		// TODO revert retries
	}
}

func (w *localWorker) taskComplete(ctx Context, taskErr *TaskError) error {
	task := ctx.Task()
	if len(task.SubTaskIDs) > 0 {
		task.State = TaskWaiting
	} else if task.ResumeTo != "" {
		task.State = TaskPending
	} else {
		task.State = TaskCompleted
	}
	if taskErr == nil || taskErr.Type == TaskErrIgnored {
		if task.Revert {
			if task.Result == TaskUnknown {
				task.Result = TaskAborted
			}
			// else respect the previous value
		} else {
			task.Result = TaskSuccess
		}
	} else {
		task.Result = TaskFailure
		task.Errors = append(task.Errors, *taskErr)
		switch taskErr.Type {
		case TaskErrFail:
			setFailureState(&task, taskErr.Cause)
		case TaskErrRetry:
			// TODO revert retries
			if task.Retries >= task.MaxRetries {
				setFailureState(&task, taskErr.Cause)
			} else {
				task.State = TaskPending
				task.Retries++
				if task.ResumeTo == "" {
					task.ResumeTo = task.Stage
				}
			}
		case TaskErrStuck:
			task.State = TaskStucked
		}
	}
	return ctx.local.handle.Update(&task)
}
