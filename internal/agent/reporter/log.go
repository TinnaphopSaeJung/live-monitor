package reporter

import (
	"context"
	"fmt"
	"time"

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
	event contracts.IncidentEvent,
) error {
	if event.ResolutionReason == nil {
		fmt.Printf(
			"[REPORT] INCIDENT machine=%s event=%s type=%s started_at=%s\n",
			event.MachineID,
			event.EventType,
			event.IncidentType,
			event.StartedAt.Format(time.RFC3339),
		)

		return nil
	}

	durationMS := int64(0)

	if event.DurationMS != nil {
		durationMS = *event.DurationMS
	}

	fmt.Printf(
		"[REPORT] INCIDENT machine=%s event=%s type=%s reason=%s duration_ms=%d\n",
		event.MachineID,
		event.EventType,
		event.IncidentType,
		*event.ResolutionReason,
		durationMS,
	)

	return nil
}
