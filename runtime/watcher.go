package runtime

import "time"

type localWatcher struct {
	id         string
	dispatcher *Dispatcher
}

func (w *localWatcher) Run(stopCh StopChan) {
	for {
		w.dispatcher.Strategy.HouseKeep(w.id, func(ctx HouseKeepContext) error {
			err := w.wakeupWaitingTasks(ctx)
			if err != nil {
				// TODO logging
			}
			return nil
		})
		select {
		case <-time.After(w.dispatcher.HouseKeepInterval):
			continue
		case <-stopCh:
			return
		}
	}
}

func (w *localWatcher) wakeupWaitingTasks(ctx HouseKeepContext) error {
	task := ctx.Task()
	if task.State != TaskWaiting {
		return nil
	}

	completes := 0
	failures := 0
	aborted := 0
	for _, taskID := range task.SubTaskIDs {
		subTask, err := w.dispatcher.Strategy.QueryTask(taskID)
		if err != nil {
			// TODO log
			return err
		}
		if subTask.State == TaskCompleted {
			completes++
			switch subTask.Result {
			case TaskFailure:
				failures++
			case TaskAborted:
				aborted++
			}
		}
	}

	if completes >= len(task.SubTaskIDs) {
		handle, err := ctx.Acquire(task.ID)
		if err != nil {
			return err
		}
		defer handle.Done()
		task = handle.Task()
		if completes >= len(task.SubTaskIDs) &&
			task.State == TaskWaiting {
			if task.ResumeTo != "" {
				task.State = TaskPending
			} else {
				task.State = TaskCompleted
				if failures > 0 {
					task.Result = TaskFailure
				} else if aborted > 0 {
					task.Result = TaskAborted
				} else {
					task.Result = TaskSuccess
				}
			}
			handle.Update(task)
		}
	}
	return nil
}
