package incident

import "time"

type Type string

const (
	TypeAudioTooLow    Type = "AUDIO_TOO_LOW"
	TypeMuted          Type = "AUDIO_MUTED"
	TypeSignalLost     Type = "SIGNAL_LOST"
	TypeRoutingInvalid Type = "ROUTING_INVALID"
)

type EventType string

const (
	EventOpened   EventType = "OPENED"
	EventResolved EventType = "RESOLVED"
)

type ResolutionReason string

const (
	ResolutionRecovered         ResolutionReason = "RECOVERED"
	ResolutionMonitoringStopped ResolutionReason = "MONITORING_STOPPED"
)

type HealthSnapshot struct {
	AudioTooLow    bool
	Muted          bool
	SignalLost     bool
	RoutingInvalid bool
}

type Event struct {
	EventType    EventType
	IncidentType Type

	StartedAt  time.Time
	OccurredAt time.Time
	Duration   time.Duration

	ResolutionReason ResolutionReason
}

type activeIncident struct {
	StartedAt time.Time
}

type Manager struct {
	active map[Type]activeIncident
}

func NewManager() *Manager {
	return &Manager{
		active: make(map[Type]activeIncident),
	}
}

func (m *Manager) Reconcile(
	monitoringEnabled bool,
	snapshot HealthSnapshot,
	at time.Time,
) []Event {
	// --------------------------------------------------
	// ไม่ได้อยู่ในช่วง Monitoring
	//
	// Incident ที่เปิดอยู่ต้องจบ
	// แต่ไม่ได้หมายความว่า Audio recover
	//
	// เช่น User กด Stop Streaming
	// --------------------------------------------------

	if !monitoringEnabled {
		return m.closeAll(
			at,
			ResolutionMonitoringStopped,
		)
	}

	// --------------------------------------------------
	// กำลัง Streaming
	//
	// เปรียบเทียบ Current Health กับ Active Incidents
	// --------------------------------------------------

	var events []Event

	for _, incidentType := range orderedIncidentTypes {
		unhealthy := snapshot.isActive(
			incidentType,
		)

		active, exists := m.active[incidentType]

		// ==============================================
		// ปัญหาเกิดขึ้น แต่ยังไม่มี Incident
		// ==============================================

		if unhealthy && !exists {
			m.active[incidentType] = activeIncident{
				StartedAt: at,
			}

			events = append(
				events,
				Event{
					EventType:    EventOpened,
					IncidentType: incidentType,
					StartedAt:    at,
					OccurredAt:   at,
				},
			)

			continue
		}

		// ==============================================
		// ปัญหาหายแล้ว และมี Incident เปิดอยู่
		// ==============================================

		if !unhealthy && exists {
			delete(
				m.active,
				incidentType,
			)

			events = append(
				events,
				Event{
					EventType:        EventResolved,
					IncidentType:     incidentType,
					StartedAt:        active.StartedAt,
					OccurredAt:       at,
					Duration:         at.Sub(active.StartedAt),
					ResolutionReason: ResolutionRecovered,
				},
			)
		}
	}

	return events
}

func (m *Manager) closeAll(
	at time.Time,
	reason ResolutionReason,
) []Event {
	var events []Event

	for _, incidentType := range orderedIncidentTypes {
		active, exists := m.active[incidentType]
		if !exists {
			continue
		}

		delete(
			m.active,
			incidentType,
		)

		events = append(
			events,
			Event{
				EventType:        EventResolved,
				IncidentType:     incidentType,
				StartedAt:        active.StartedAt,
				OccurredAt:       at,
				Duration:         at.Sub(active.StartedAt),
				ResolutionReason: reason,
			},
		)
	}

	return events
}

func (s HealthSnapshot) isActive(
	incidentType Type,
) bool {
	switch incidentType {

	case TypeAudioTooLow:
		return s.AudioTooLow

	case TypeMuted:
		return s.Muted

	case TypeSignalLost:
		return s.SignalLost

	case TypeRoutingInvalid:
		return s.RoutingInvalid

	default:
		return false
	}
}

var orderedIncidentTypes = []Type{
	TypeSignalLost,
	TypeMuted,
	TypeAudioTooLow,
	TypeRoutingInvalid,
}

func (m *Manager) ActiveTypes() []Type {
	active := make(
		[]Type,
		0,
		len(m.active),
	)

	for _, incidentType := range orderedIncidentTypes {
		if _, exists := m.active[incidentType]; !exists {
			continue
		}

		active = append(
			active,
			incidentType,
		)
	}

	return active
}
