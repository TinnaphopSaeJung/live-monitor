package contracts

import "time"

type AudioHealth struct {
	SignalState  string `json:"signal_state"`
	LevelState   string `json:"level_state"`
	MuteState    string `json:"mute_state"`
	RoutingState string `json:"routing_state"`
	SampleState  string `json:"sample_state"`
}

type Heartbeat struct {
	MachineID string    `json:"machine_id"`
	SentAt    time.Time `json:"sent_at"`

	OBSConnected bool `json:"obs_connected"`

	StreamState      string `json:"stream_state"`
	MonitoringActive bool   `json:"monitoring_active"`

	Audio AudioHealth `json:"audio"`

	ActiveIncidents []string `json:"active_incidents"`
}
