package contracts

import "time"

type IncidentEvent struct {
	EventID   string `json:"event_id"`
	MachineID string `json:"machine_id"`

	EventType    string `json:"event_type"`
	IncidentType string `json:"incident_type"`

	StartedAt  time.Time `json:"started_at"`
	OccurredAt time.Time `json:"occurred_at"`

	ResolutionReason *string `json:"resolution_reason,omitempty"`
	DurationMS       *int64  `json:"duration_ms,omitempty"`
}
