package runtime

import "encoding/json"

// Context provides the context for a running task
type Context struct {
	local *localContext
}

// Task returns a copy of current task
func (c Context) Task() Task {
	return *c.local.taskHandle().Task()
}

// Job returns a copy of current job
func (c Context) Job() (Job, error) {
	return c.local.dispatcher().Job(c.JobID())
}

// Parent returns a copy of parent task
func (c Context) Parent() (Task, error) {
	return c.local.dispatcher().Task(c.ParentID())
}

// SubTasks retrieves sub tasks
func (c Context) SubTasks() ([]Task, error) {
	t := c.Task()
	tasks := make([]Task, 0, len(t.SubTaskIDs))
	for _, id := range t.SubTaskIDs {
		task, err := c.TaskByID(id)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks, nil
}

// JobID retrieves the current job id
func (c Context) JobID() string {
	return c.Task().JobID
}

// TaskID retrieves the current task id
func (c Context) TaskID() string {
	return c.Task().ID
}

// ParentID retrieves the parent task id
func (c Context) ParentID() string {
	return c.Task().ParentID
}

// StopCh returns the stopChan
func (c Context) StopCh() StopChan {
	return c.local.stopCh
}

// IsRollback determines if the task is in rollback direction
func (c Context) IsRollback() bool {
	return c.Task().Revert
}

// IsCanceling determines if cancellation is requested
func (c Context) IsCanceling() bool {
	canceling, err := c.local.strategy().IsJobCanceling(c.JobID())
	return err == nil && canceling
}

// TaskByID queries task by id
func (c Context) TaskByID(id string) (Task, error) {
	return c.local.dispatcher().Task(id)
}

// JobByID queries job by id
func (c Context) JobByID(id string) (Job, error) {
	return c.local.dispatcher().Job(id)
}

// GetParams extracts the parameters for current task
func (c Context) GetParams(p interface{}) error {
	params := c.Task().Params
	if params == nil {
		return nil
	}
	return json.Unmarshal(params, p)
}

// SetData saves the data of the task
func (c Context) SetData(p interface{}) (err error) {
	t := c.Task()
	return c.local.taskHandle().Update(t.SetData(p))
}

// SetOutput saves the output of the task
func (c Context) SetOutput(p interface{}) (err error) {
	t := c.Task()
	return c.local.taskHandle().Update(t.SetOutput(p))
}

// ResumeTo specifies the next stage when sub tasks finish
func (c Context) ResumeTo(stage string) error {
	t := c.Task()
	t.ResumeTo = stage
	return c.local.taskHandle().Update(&t)
}

// NewTask starts creating a new sub task
func (c Context) NewTask(name string) *TaskBuilder {
	return &TaskBuilder{Submitter: c, Name: name}
}

// Fail creates a task error
func (c Context) Fail(err error) *TaskError {
	t := c.Task()
	return t.NewError(TaskErrFail).SetMessage("failed").CausedBy(err)
}

// FailRetry creates a task error with retry
func (c Context) FailRetry(err error) *TaskError {
	t := c.Task()
	return t.NewError(TaskErrRetry).SetMessage("error").CausedBy(err)
}

// Stuck creates a stuck error
func (c Context) Stuck(err error) *TaskError {
	t := c.Task()
	return t.NewError(TaskErrStuck).SetMessage("stucked!!").CausedBy(err)
}

// SubmitTask implements TaskSubmitter
func (c Context) SubmitTask(task *Task) error {
	task.JobID = c.JobID()
	task.ParentID = c.TaskID()
	return c.local.taskHandle().SubmitTask(task)
}
