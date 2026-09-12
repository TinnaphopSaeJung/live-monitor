package reporter

import (
	"context"

	"live-monitor/internal/agent/incident"
	"live-monitor/internal/contracts"
)

type Reporter interface {
	SendHeartbeat(
		ctx context.Context,
		heartbeat contracts.Heartbeat,
	) error

	SendIncident(
		ctx context.Context,
		event incident.Event,
	) error
}
