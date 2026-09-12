package reporter

import (
	"context"
	"fmt"

	"live-monitor/internal/agent/incident"
	"live-monitor/internal/contracts"
)

type LogReporter struct{}

func NewLogReporter() *LogReporter {
	return &LogReporter{}
}

func (r *LogReporter) SendHeartbeat(
	_ context.Context,
	heartbeat contracts.Heartbeat,
) error {
	fmt.Printf(
		"[REPORT] HEARTBEAT machine=%s stream=%s monitoring=%t incidents=%v\n",
		heartbeat.MachineID,
		heartbeat.StreamState,
		heartbeat.MonitoringActive,
		heartbeat.ActiveIncidents,
	)

	return nil
}

func (r *LogReporter) SendIncident(
	_ context.Context,
	event incident.Event,
) error {
	fmt.Printf(
		"[REPORT] INCIDENT event=%s type=%s reason=%s duration=%s\n",
		event.EventType,
		event.IncidentType,
		event.ResolutionReason,
		event.Duration,
	)

	return nil
}
