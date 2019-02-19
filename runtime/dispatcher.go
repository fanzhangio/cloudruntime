package runtime

import (
	"sync"
	"time"
)

// Dispatcher submits jobs and executes tasks
type Dispatcher struct {
	Strategy          Strategy
	Tasks             []*TaskExec
	HouseKeepInterval time.Duration

	workers   map[string]*controller
	watchers  map[string]*controller
	runners   []*controller
	wgRunners sync.WaitGroup
	lock      sync.Mutex
}

// StopChan is the chan delivering stop signal
type StopChan <-chan struct{}

// Runnable defines the generic background runner
type Runnable interface {
	Run(StopChan)
}

// Worker executes tasks
type Worker interface {
	Runnable
}

// Watcher watches the system and does house keeping
type Watcher interface {
	Runnable
}

// HouseKeepInterval is the default setting
var HouseKeepInterval = time.Second

type controller struct {
	runner Runnable
	stopCh chan struct{}
}

func newController(runner Runnable) *controller {
	return &controller{runner: runner, stopCh: make(chan struct{})}
}

func (r *controller) run() {
	r.runner.Run(r.stopCh)
}

func (r *controller) stop() {
	close(r.stopCh)
}

// NewDispatcher creates a Dispatcher
func NewDispatcher(strategy Strategy) *Dispatcher {
	return &Dispatcher{
		Strategy:          strategy,
		HouseKeepInterval: HouseKeepInterval,
	}
}

// NewJob starts creating a job
func (d *Dispatcher) NewJob() *JobBuilder {
	return &JobBuilder{Submitter: d}
}

// SubmitJob implements JobSubmitter
func (d *Dispatcher) SubmitJob(job *Job) error {
	// TODO validate job
	return d.Strategy.SubmitJob(job)
}

// AddTaskExecs adds task executors
func (d *Dispatcher) AddTaskExecs(execs ...*TaskExec) {
	d.Tasks = append(d.Tasks, execs...)
}

// NewTaskExec defines a new task executor
func (d *Dispatcher) NewTaskExec(taskName string) *TaskExecBuilder {
	return &TaskExecBuilder{Dispatcher: d, Executor: TaskExec{Name: taskName}}
}

// Worker creates a worker
func (d *Dispatcher) Worker(id string) Worker {
	d.lock.Lock()
	defer d.lock.Unlock()
	if d.workers == nil {
		d.workers = make(map[string]*controller)
	}
	rctl := d.workers[id]
	if rctl == nil {
		rctl = newController(&localWorker{
			dispatcher: d,
			strategy:   d.Strategy.NewWorker(id),
		})
		d.workers[id] = rctl
	}
	return rctl.runner.(Worker)
}

// Watcher creates a watcher
func (d *Dispatcher) Watcher(id string) Watcher {
	d.lock.Lock()
	defer d.lock.Unlock()
	if d.watchers == nil {
		d.watchers = make(map[string]*controller)
	}
	rctl := d.watchers[id]
	if rctl == nil {
		rctl = newController(&localWatcher{
			id:         id,
			dispatcher: d,
		})
		d.watchers[id] = rctl
	}
	return rctl.runner.(Watcher)
}

// Start starts workers and background tasks
func (d *Dispatcher) Start() *Dispatcher {
	d.lock.Lock()
	d.wgRunners = sync.WaitGroup{}
	d.runners = make([]*controller, 0)
	if d.workers != nil {
		for _, rctl := range d.workers {
			d.runners = append(d.runners, rctl)
			d.wgRunners.Add(1)
		}
	}
	if d.watchers != nil {
		for _, rctl := range d.watchers {
			d.runners = append(d.runners, rctl)
			d.wgRunners.Add(1)
		}
	}
	for _, rctl := range d.runners {
		go func(r *controller) {
			r.run()
			d.wgRunners.Done()
		}(rctl)
	}
	d.lock.Unlock()
	return d
}

// Stop notifies workers and background tasks to exit
func (d *Dispatcher) Stop() *Dispatcher {
	d.lock.Lock()
	for _, rctl := range d.runners {
		rctl.stop()
	}
	d.lock.Unlock()
	return d
}

// Wait waits until workers and background tasks complete
func (d *Dispatcher) Wait() *Dispatcher {
	d.wgRunners.Wait()
	return d
}

// Run is equilavent to Start and Wait
func (d *Dispatcher) Run() *Dispatcher {
	d.Start()
	d.Wait()
	return d
}

// Task queries task by id
func (d *Dispatcher) Task(id string) (Task, error) {
	task, err := d.Strategy.QueryTask(id)
	if err != nil {
		return Task{}, err
	}
	if task == nil {
		return Task{}, NotExist(id)
	}
	return *task, nil
}

// Job queries job by id
func (d *Dispatcher) Job(id string) (Job, error) {
	job, err := d.Strategy.QueryJob(id)
	if err != nil {
		return Job{}, err
	}
	if job == nil {
		return Job{}, NotExist(id)
	}
	return *job, nil
}

func (d *Dispatcher) findStage(name, stage string) *Stage {
	for _, t := range d.Tasks {
		if t.Name != name || len(t.Stages) == 0 {
			continue
		}
		if stage == "" {
			return &t.Stages[0]
		}
		for _, s := range t.Stages {
			if s.Name == stage {
				return &s
			}
		}
	}
	return nil
}
