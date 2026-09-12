package detector

import (
	"time"

	"live-monitor/internal/agent/audio"
)

type MuteState string

const (
	MuteStateHealthy MuteState = "HEALTHY"
	MuteStateMuted   MuteState = "MUTED"
)

type MuteTransition string

const (
	MuteTransitionNone    MuteTransition = ""
	MuteTransitionMuted   MuteTransition = "MUTED"
	MuteTransitionUnmuted MuteTransition = "UNMUTED"
)

type MuteResult struct {
	State      MuteState
	Transition MuteTransition

	MutedDuration time.Duration
}

type MuteDetector struct {
	state MuteState

	mutedSince time.Time
}

func NewMuteDetector() *MuteDetector {
	return &MuteDetector{
		state: MuteStateHealthy,
	}
}

func (d *MuteDetector) Process(
	sample audio.Sample,
) MuteResult {
	result := MuteResult{
		State: d.state,
	}

	// --------------------------------------------------
	// Input ถูก Mute
	// --------------------------------------------------

	if sample.Muted {
		// ถ้าอยู่ MUTED อยู่แล้ว
		// ไม่ต้อง emit transition ซ้ำ
		if d.state == MuteStateMuted {
			return result
		}

		// HEALTHY → MUTED
		d.state = MuteStateMuted
		d.mutedSince = sample.Timestamp

		return MuteResult{
			State:      d.state,
			Transition: MuteTransitionMuted,
		}
	}

	// --------------------------------------------------
	// Input ไม่ได้ Mute
	// --------------------------------------------------

	// ถ้าเดิมก็ HEALTHY อยู่แล้ว
	// ไม่มีอะไรต้องทำ
	if d.state == MuteStateHealthy {
		return result
	}

	// --------------------------------------------------
	// MUTED → HEALTHY
	// --------------------------------------------------

	mutedFor := sample.Timestamp.Sub(
		d.mutedSince,
	)

	d.state = MuteStateHealthy
	d.mutedSince = time.Time{}

	return MuteResult{
		State:         d.state,
		Transition:    MuteTransitionUnmuted,
		MutedDuration: mutedFor,
	}
}

func (d *MuteDetector) State() MuteState {
	return d.state
}
