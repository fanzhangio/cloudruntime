package runtime

// Strategy is the contract for scheduling strategy
type Strategy interface {
	SubmitJob(*Job) error
	CancelJob(id string) error
	IsJobCanceling(id string) (bool, error)
	QueryJob(id string) (*Job, error)
	QueryTask(id string) (*Task, error)
	NewWorker(id string) WorkerStrategy
	HouseKeep(id string, logic HouseKeepLogic) error
}

// WorkerStrategy is strategy instance per worker
type WorkerStrategy interface {
	FetchTask() (TaskHandle, error)
}

// TaskHandle is the handle of a running task owned by a worker
type TaskHandle interface {
	Task() *Task
	SubmitTask(*Task) error
	Update(*Task) error
	Done() error
}

// HouseKeepContext provides context for HouseKeepLogic
type HouseKeepContext interface {
	Job() *Job
	Task() *Task
	Acquire(taskID string) (TaskHandle, error)
	Stop()
}

// HouseKeepLogic runs house keep for one task
type HouseKeepLogic func(HouseKeepContext) error
