package runtime

import "time"

// Job defines the details of a job
type Job struct {
	ID        string    `json:"id"`         // globally unique job id
	Name      string    `json:"name"`       // job name, optionally
	Task      *Task     `json:"task"`       // the entry task
	CreatedAt time.Time `json:"created-at"` // job creation time
	UpdatedAt time.Time `json:"updated-at"` // last updated time
}

// JobSubmitter defines the contract which submits a job
type JobSubmitter interface {
	SubmitJob(*Job) error
}

// JobBuilder is the helper creates a job
type JobBuilder struct {
	Submitter JobSubmitter
	ID        string
	Name      string
	Task      *Task
}

// SetID specifies the globally unique job id
func (b *JobBuilder) SetID(id string) *JobBuilder {
	b.ID = id
	return b
}

// SetName specifies the job name
func (b *JobBuilder) SetName(name string) *JobBuilder {
	b.Name = name
	return b
}

// SetTask specifies the entry task
func (b *JobBuilder) SetTask(task *Task) *JobBuilder {
	b.Task = task
	return b
}

// Submit submits the job for execution
func (b *JobBuilder) Submit() (*Job, error) {
	job := &Job{
		ID:   b.ID,
		Name: b.Name,
		Task: b.Task,
	}
	if job.ID == "" {
		// TODO generate ID
	}
	job.Task.JobID = job.ID
	job.Task.CreatedAt = time.Now()
	job.Task.UpdatedAt = job.Task.CreatedAt
	job.CreatedAt = job.Task.CreatedAt
	job.UpdatedAt = job.CreatedAt
	return job, b.Submitter.SubmitJob(job)
}
