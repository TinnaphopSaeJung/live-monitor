package repository

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"live-monitor/internal/contracts"
)

type IncidentRepository struct {
	db *pgxpool.Pool
}

type ProcessIncidentResult struct {
	Duplicate    bool
	LineNotified bool
}

func NewIncidentRepository(
	db *pgxpool.Pool,
) *IncidentRepository {
	return &IncidentRepository{
		db: db,
	}
}

func (r *IncidentRepository) ProcessEvent(
	ctx context.Context,
	event contracts.IncidentEvent,
) (ProcessIncidentResult, error) {
	// --------------------------------------------------
	// Begin transaction
	// --------------------------------------------------

	tx, err := r.db.Begin(ctx)
	if err != nil {
		return ProcessIncidentResult{},
			fmt.Errorf(
				"begin incident transaction: %w",
				err,
			)
	}

	defer func() {
		_ = tx.Rollback(ctx)
	}()

	// --------------------------------------------------
	// 1. Ensure machine exists
	//
	// Incident อาจมาถึงก่อน heartbeat
	// --------------------------------------------------

	_, err = tx.Exec(
		ctx,
		`
			INSERT INTO machines (
				machine_id
			)
			VALUES ($1)
			ON CONFLICT (machine_id)
			DO NOTHING
		`,
		event.MachineID,
	)
	if err != nil {
		return ProcessIncidentResult{},
			fmt.Errorf(
				"ensure machine exists: %w",
				err,
			)
	}

	// --------------------------------------------------
	// 2. Insert raw incident event
	//
	// event_id เป็น PRIMARY KEY
	//
	// ถ้า Retry event เดิมเข้ามา:
	// ON CONFLICT DO NOTHING
	// --------------------------------------------------

	tag, err := tx.Exec(
		ctx,
		`
			INSERT INTO incident_events (
				event_id,
				machine_id,
				event_type,
				incident_type,
				started_at,
				occurred_at,
				resolution_reason,
				duration_ms
			)
			VALUES (
				$1,
				$2,
				$3,
				$4,
				$5,
				$6,
				$7,
				$8
			)
			ON CONFLICT (event_id)
			DO NOTHING
		`,
		event.EventID,
		event.MachineID,
		event.EventType,
		event.IncidentType,
		event.StartedAt,
		event.OccurredAt,
		event.ResolutionReason,
		event.DurationMS,
	)
	if err != nil {
		return ProcessIncidentResult{},
			fmt.Errorf(
				"insert incident event: %w",
				err,
			)
	}

	// --------------------------------------------------
	// Duplicate event
	//
	// RowsAffected == 0
	// แปลว่า event_id นี้มีอยู่แล้ว
	// --------------------------------------------------

	if tag.RowsAffected() == 0 {
		var lineNotified bool

		err := tx.QueryRow(
			ctx,
			`
			SELECT line_notified_at IS NOT NULL
			FROM incident_events
			WHERE event_id = $1
		`,
			event.EventID,
		).Scan(
			&lineNotified,
		)
		if err != nil {
			return ProcessIncidentResult{},
				fmt.Errorf(
					"get LINE notification state: %w",
					err,
				)
		}

		if err := tx.Commit(ctx); err != nil {
			return ProcessIncidentResult{},
				fmt.Errorf(
					"commit duplicate incident transaction: %w",
					err,
				)
		}

		return ProcessIncidentResult{
			Duplicate:    true,
			LineNotified: lineNotified,
		}, nil
	}

	// --------------------------------------------------
	// 3. Update incidents table
	// --------------------------------------------------

	switch event.EventType {

	case "OPENED":
		err = processOpenedIncident(
			ctx,
			tx,
			event,
		)

	case "RESOLVED":
		err = processResolvedIncident(
			ctx,
			tx,
			event,
		)

	default:
		return ProcessIncidentResult{},
			fmt.Errorf(
				"unsupported incident event type: %s",
				event.EventType,
			)
	}

	if err != nil {
		return ProcessIncidentResult{},
			err
	}

	// --------------------------------------------------
	// Commit
	// --------------------------------------------------

	if err := tx.Commit(ctx); err != nil {
		return ProcessIncidentResult{},
			fmt.Errorf(
				"commit incident transaction: %w",
				err,
			)
	}

	return ProcessIncidentResult{
		Duplicate:    false,
		LineNotified: false,
	}, nil
}

func processOpenedIncident(
	ctx context.Context,
	tx pgx.Tx,
	event contracts.IncidentEvent,
) error {
	_, err := tx.Exec(
		ctx,
		`
			INSERT INTO incidents (
				machine_id,
				incident_type,
				status,
				started_at
			)
			VALUES (
				$1,
				$2,
				'OPEN',
				$3
			)
			ON CONFLICT (
				machine_id,
				incident_type,
				started_at
			)
			DO NOTHING
		`,
		event.MachineID,
		event.IncidentType,
		event.StartedAt,
	)
	if err != nil {
		return fmt.Errorf(
			"open incident: %w",
			err,
		)
	}

	return nil
}

func processResolvedIncident(
	ctx context.Context,
	tx pgx.Tx,
	event contracts.IncidentEvent,
) error {
	if event.ResolutionReason == nil {
		return fmt.Errorf(
			"resolution_reason is required for RESOLVED event",
		)
	}

	if event.DurationMS == nil {
		return fmt.Errorf(
			"duration_ms is required for RESOLVED event",
		)
	}

	_, err := tx.Exec(
		ctx,
		`
			INSERT INTO incidents (
				machine_id,
				incident_type,
				status,
				started_at,
				resolved_at,
				resolution_reason,
				duration_ms
			)
			VALUES (
				$1,
				$2,
				'RESOLVED',
				$3,
				$4,
				$5,
				$6
			)
			ON CONFLICT (
				machine_id,
				incident_type,
				started_at
			)
			DO UPDATE SET
				status = 'RESOLVED',
				resolved_at = EXCLUDED.resolved_at,
				resolution_reason =
					EXCLUDED.resolution_reason,
				duration_ms =
					EXCLUDED.duration_ms,
				updated_at = NOW()
		`,
		event.MachineID,
		event.IncidentType,
		event.StartedAt,
		event.OccurredAt,
		*event.ResolutionReason,
		*event.DurationMS,
	)
	if err != nil {
		return fmt.Errorf(
			"resolve incident: %w",
			err,
		)
	}

	return nil
}

func (r *IncidentRepository) MarkLineNotified(
	ctx context.Context,
	eventID string,
) error {
	tag, err := r.db.Exec(
		ctx,
		`
			UPDATE incident_events
			SET line_notified_at = NOW()
			WHERE event_id = $1
		`,
		eventID,
	)
	if err != nil {
		return fmt.Errorf(
			"mark LINE notification sent: %w",
			err,
		)
	}

	if tag.RowsAffected() == 0 {
		return fmt.Errorf(
			"incident event not found: %s",
			eventID,
		)
	}

	return nil
}
