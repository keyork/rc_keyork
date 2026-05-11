package model

import "time"

type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusSuccess    Status = "success"
	StatusFailed     Status = "failed"
)

type Notification struct {
	ID       string            `json:"id"`
	TargetURL string           `json:"target_url"`
	Method   string            `json:"method"`
	Headers  map[string]string `json:"headers"`
	Body     string            `json:"body,omitempty"`

	Status      Status `json:"status"`
	RetryCount  int    `json:"retry_count"`
	MaxRetries  int    `json:"max_retries"`
	NextRetryAt *time.Time `json:"next_retry_at,omitempty"`

	CallbackURL    string `json:"callback_url,omitempty"`
	CallbackStatus string `json:"callback_status,omitempty"`

	LastHTTPStatus int    `json:"last_http_status,omitempty"`
	LastError      string `json:"last_error,omitempty"`

	SourceSystem string `json:"source_system,omitempty"`
	TargetDomain string `json:"target_domain"`

	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
}

// SubmitRequest is the inbound payload from business systems.
type SubmitRequest struct {
	TargetURL    string            `json:"target_url"`
	Method       string            `json:"method"`
	Headers      map[string]string `json:"headers"`
	Body         string            `json:"body"`
	CallbackURL  string            `json:"callback_url"`
	SourceSystem string            `json:"source_system"`
}

// ListFilter is used by the query endpoint.
type ListFilter struct {
	Status string
	Domain string
	From   time.Time
	To     time.Time
	Page   int
	Size   int
}

// DeliveryMessage is what gets published to / consumed from the MQ.
type DeliveryMessage struct {
	NotificationID string `json:"notification_id"`
	RetryCount     int    `json:"retry_count"`
}
