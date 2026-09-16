package model

type JobStatus string

const (
	JobStatusCreated JobStatus = "created"
	JobStatusPending JobStatus = "pending"
	JobStatusDone    JobStatus = "done"
	JobStatusFailed  JobStatus = "failed"
)

type Job struct {
	ID              int
	OriginKey       string
	CurrentStateKey string
	Status          JobStatus
	IsEqual         bool
	Details         string
}

type JobStatusResponse struct {
	ID        int       `json:"id"`
	JobStatus JobStatus `json:"job_status"`
	IsEqual   bool      `json:"is_equal"`
	Details   string    `json:"details,omitempty"`
}

type JobKafkaMsg struct {
	ID              int    `json:"id"`
	OriginKey       string `json:"origin_key"`
	CurrentStateKey string `json:"current_state_key"`
}
