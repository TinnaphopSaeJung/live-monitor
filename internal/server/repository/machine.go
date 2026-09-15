package repository

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"live-monitor/internal/contracts"
)

type MachineRepository struct {
	db *pgxpool.Pool
}

func NewMachineRepository(
	db *pgxpool.Pool,
) *MachineRepository {
	return &MachineRepository{
		db: db,
	}
}

func (r *MachineRepository) UpsertHeartbeat(
	ctx context.Context,
	heartbeat contracts.Heartbeat,
) error {
	const query = `
		INSERT INTO machines (
			machine_id,
			last_seen_at,
			agent_sent_at,
			obs_connected,
			stream_state,
			monitoring_active,
			signal_state,
			level_state,
			mute_state,
			routing_state,
			updated_at
		)
		VALUES (
			$1,
			NOW(),
			$2,
			$3,
			$4,
			$5,
			$6,
			$7,
			$8,
			$9,
			NOW()
		)
		ON CONFLICT (machine_id)
		DO UPDATE SET
			last_seen_at = NOW(),
			agent_sent_at = EXCLUDED.agent_sent_at,
			obs_connected = EXCLUDED.obs_connected,
			stream_state = EXCLUDED.stream_state,
			monitoring_active = EXCLUDED.monitoring_active,
			signal_state = EXCLUDED.signal_state,
			level_state = EXCLUDED.level_state,
			mute_state = EXCLUDED.mute_state,
			routing_state = EXCLUDED.routing_state,
			updated_at = NOW()
	`

	_, err := r.db.Exec(
		ctx,
		query,

		heartbeat.MachineID,
		heartbeat.SentAt,
		heartbeat.OBSConnected,
		heartbeat.StreamState,
		heartbeat.MonitoringActive,

		heartbeat.Audio.SignalState,
		heartbeat.Audio.LevelState,
		heartbeat.Audio.MuteState,
		heartbeat.Audio.RoutingState,
	)
	if err != nil {
		return fmt.Errorf(
			"upsert machine heartbeat: %w",
			err,
		)
	}

	return nil
}
