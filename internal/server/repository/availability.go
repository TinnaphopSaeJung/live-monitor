package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type AvailabilityRepository struct {
	db *pgxpool.Pool
}

type UnreachableMachine struct {
	MachineID  string
	LastSeenAt time.Time
}

type RecoveredMachine struct {
	MachineID  string
	StartedAt  time.Time
	LastSeenAt time.Time
}

func NewAvailabilityRepository(
	db *pgxpool.Pool,
) *AvailabilityRepository {
	return &AvailabilityRepository{
		db: db,
	}
}

// --------------------------------------------------
// หาเครื่องที่:
//
// 1. ก่อนหายไปกำลัง Monitoring อยู่
// 2. last_seen เก่ากว่า cutoff
// 3. ยังไม่มี AGENT_UNREACHABLE ที่ OPEN อยู่
// --------------------------------------------------

func (r *AvailabilityRepository) FindUnreachableMachines(
	ctx context.Context,
	cutoff time.Time,
) ([]UnreachableMachine, error) {
	const query = `
		SELECT
			m.machine_id,
			m.last_seen_at
		FROM machines m
		WHERE
			m.monitoring_active = TRUE
			AND m.last_seen_at < $1
			AND NOT EXISTS (
				SELECT 1
				FROM incidents i
				WHERE
					i.machine_id = m.machine_id
					AND i.incident_type = 'AGENT_UNREACHABLE'
					AND i.status = 'OPEN'
			)
	`

	rows, err := r.db.Query(
		ctx,
		query,
		cutoff,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"find unreachable machines: %w",
			err,
		)
	}
	defer rows.Close()

	var machines []UnreachableMachine

	for rows.Next() {
		var machine UnreachableMachine

		if err := rows.Scan(
			&machine.MachineID,
			&machine.LastSeenAt,
		); err != nil {
			return nil, fmt.Errorf(
				"scan unreachable machine: %w",
				err,
			)
		}

		machines = append(
			machines,
			machine,
		)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate unreachable machines: %w",
			err,
		)
	}

	return machines, nil
}

// --------------------------------------------------
// หา AGENT_UNREACHABLE ที่ยัง OPEN
//
// แต่ตอนนี้ Machine มี heartbeat ใหม่กลับมาแล้ว
// --------------------------------------------------

func (r *AvailabilityRepository) FindRecoveredMachines(
	ctx context.Context,
	cutoff time.Time,
) ([]RecoveredMachine, error) {
	const query = `
		SELECT
			i.machine_id,
			i.started_at,
			m.last_seen_at
		FROM incidents i
		JOIN machines m
			ON m.machine_id = i.machine_id
		WHERE
			i.incident_type = 'AGENT_UNREACHABLE'
			AND i.status = 'OPEN'
			AND m.last_seen_at >= $1
	`

	rows, err := r.db.Query(
		ctx,
		query,
		cutoff,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"find recovered machines: %w",
			err,
		)
	}
	defer rows.Close()

	var machines []RecoveredMachine

	for rows.Next() {
		var machine RecoveredMachine

		if err := rows.Scan(
			&machine.MachineID,
			&machine.StartedAt,
			&machine.LastSeenAt,
		); err != nil {
			return nil, fmt.Errorf(
				"scan recovered machine: %w",
				err,
			)
		}

		machines = append(
			machines,
			machine,
		)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate recovered machines: %w",
			err,
		)
	}

	return machines, nil
}
