package monitor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"live-monitor/internal/contracts"
	"live-monitor/internal/server/repository"
	"live-monitor/internal/server/service"
	"log"
	"time"
)

const agentUnreachableIncidentType = "AGENT_UNREACHABLE"

type AvailabilityMonitor struct {
	availabilityRepository *repository.AvailabilityRepository
	incidentService        *service.IncidentService

	unreachableThreshold time.Duration
	checkInterval        time.Duration
	startupGrace         time.Duration
}

func NewAvailabilityMonitor(
	availabilityRepository *repository.AvailabilityRepository,
	incidentService *service.IncidentService,
	unreachableThreshold time.Duration,
	checkInterval time.Duration,
	startupGrace time.Duration,
) *AvailabilityMonitor {
	return &AvailabilityMonitor{
		availabilityRepository: availabilityRepository,
		incidentService:        incidentService,

		unreachableThreshold: unreachableThreshold,
		checkInterval:        checkInterval,
		startupGrace:         startupGrace,
	}
}

func (m *AvailabilityMonitor) Run(
	ctx context.Context,
) {
	// --------------------------------------------------
	// Startup Grace
	//
	// สำคัญ:
	// ถ้า Backend เพิ่ง restart
	// last_seen_at ใน DB อาจเก่ามาก
	//
	// เราให้ Agent มีเวลาส่ง Heartbeat ใหม่ก่อน
	// ไม่อย่างนั้น Backend restart อาจสร้าง false alert
	// --------------------------------------------------

	startupTimer := time.NewTimer(
		m.startupGrace,
	)
	defer startupTimer.Stop()

	select {
	case <-ctx.Done():
		return

	case <-startupTimer.C:
	}

	log.Printf(
		"Availability monitor started threshold=%s interval=%s",
		m.unreachableThreshold,
		m.checkInterval,
	)

	ticker := time.NewTicker(
		m.checkInterval,
	)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			m.check(ctx)
		}
	}
}

func (m *AvailabilityMonitor) check(
	ctx context.Context,
) {
	now := time.Now()

	cutoff := now.Add(
		-m.unreachableThreshold,
	)

	// --------------------------------------------------
	// 1. Detect unreachable
	// --------------------------------------------------

	unreachableMachines, err :=
		m.availabilityRepository.FindUnreachableMachines(
			ctx,
			cutoff,
		)
	if err != nil {
		log.Printf(
			"AVAILABILITY CHECK ERROR: %v",
			err,
		)

		return
	}

	for _, machine := range unreachableMachines {
		if err := m.openUnreachableIncident(
			ctx,
			machine,
			now,
		); err != nil {

			log.Printf(
				"AGENT UNREACHABLE OPEN ERROR machine=%s error=%v",
				machine.MachineID,
				err,
			)
		}
	}

	// --------------------------------------------------
	// 2. Detect recovery
	// --------------------------------------------------

	recoveredMachines, err :=
		m.availabilityRepository.FindRecoveredMachines(
			ctx,
			cutoff,
		)
	if err != nil {
		log.Printf(
			"AVAILABILITY RECOVERY CHECK ERROR: %v",
			err,
		)

		return
	}

	for _, machine := range recoveredMachines {
		if err := m.resolveUnreachableIncident(
			ctx,
			machine,
		); err != nil {

			log.Printf(
				"AGENT UNREACHABLE RESOLVE ERROR machine=%s error=%v",
				machine.MachineID,
				err,
			)
		}
	}
}

func (m *AvailabilityMonitor) openUnreachableIncident(
	ctx context.Context,
	machine repository.UnreachableMachine,
	detectedAt time.Time,
) error {
	// เราถือว่าเริ่ม unreachable
	// เมื่อเลย threshold จาก heartbeat ล่าสุด
	startedAt := machine.LastSeenAt.Add(
		m.unreachableThreshold,
	)

	event := contracts.IncidentEvent{
		EventID: buildAvailabilityEventID(
			machine.MachineID,
			"OPENED",
			startedAt,
		),

		MachineID: machine.MachineID,

		EventType: "OPENED",

		IncidentType: agentUnreachableIncidentType,

		StartedAt:  startedAt,
		OccurredAt: detectedAt,
	}

	result, err := m.incidentService.ProcessEvent(
		ctx,
		event,
	)
	if err != nil {
		return fmt.Errorf(
			"process unreachable incident: %w",
			err,
		)
	}

	if result.Duplicate {
		return nil
	}

	log.Printf(
		"AGENT UNREACHABLE machine=%s last_seen=%s unreachable_since=%s",
		machine.MachineID,
		machine.LastSeenAt.Format(
			time.RFC3339,
		),
		startedAt.Format(
			time.RFC3339,
		),
	)

	return nil
}

func (m *AvailabilityMonitor) resolveUnreachableIncident(
	ctx context.Context,
	machine repository.RecoveredMachine,
) error {
	reason := "HEARTBEAT_RESUMED"

	durationMS := machine.LastSeenAt.
		Sub(machine.StartedAt).
		Milliseconds()

	if durationMS < 0 {
		durationMS = 0
	}

	event := contracts.IncidentEvent{
		EventID: buildAvailabilityEventID(
			machine.MachineID,
			"RESOLVED",
			machine.StartedAt,
		),

		MachineID: machine.MachineID,

		EventType: "RESOLVED",

		IncidentType: agentUnreachableIncidentType,

		StartedAt:  machine.StartedAt,
		OccurredAt: machine.LastSeenAt,

		ResolutionReason: &reason,
		DurationMS:       &durationMS,
	}

	result, err := m.incidentService.ProcessEvent(
		ctx,
		event,
	)
	if err != nil {
		return fmt.Errorf(
			"process unreachable recovery: %w",
			err,
		)
	}

	if result.Duplicate {
		return nil
	}

	log.Printf(
		"AGENT REACHABLE machine=%s recovered_at=%s unavailable_for=%s",
		machine.MachineID,
		machine.LastSeenAt.Format(
			time.RFC3339,
		),
		time.Duration(
			durationMS,
		)*time.Millisecond,
	)

	return nil
}

func buildAvailabilityEventID(
	machineID string,
	eventType string,
	startedAt time.Time,
) string {
	raw := fmt.Sprintf(
		"%s|%s|%s|%d",
		machineID,
		agentUnreachableIncidentType,
		eventType,
		startedAt.UnixNano(),
	)

	sum := sha256.Sum256(
		[]byte(raw),
	)

	return hex.EncodeToString(
		sum[:16],
	)
}
