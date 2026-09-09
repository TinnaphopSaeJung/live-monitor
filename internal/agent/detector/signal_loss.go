package detector

import (
	"fmt"
	"time"

	"live-monitor/internal/agent/audio"
)

type State string

const (
	StateHealthy    State = "HEALTHY"
	StateSignalLost State = "SIGNAL_LOST"
)

type Transition string

const (
	TransitionNone       Transition = ""
	TransitionSignalLost Transition = "SIGNAL_LOST"
	TransitionRecovered  Transition = "RECOVERED"
)

type Config struct {
	SignalLossDuration time.Duration
	RecoveryDuration   time.Duration
}

type Result struct {
	State      State
	Transition Transition

	// ระยะเวลาตั้งแต่ไม่มี signal ครั้งแรก
	LossDuration time.Duration

	// ระยะเวลาตั้งแต่ incident ถูก confirm
	IncidentDuration time.Duration
}

type SignalLossDetector struct {
	config Config

	state State

	// เวลาเริ่มไม่มี signal
	lossSince time.Time

	// เวลาที่ครบ SignalLossDuration
	// และเปลี่ยนเป็น SIGNAL_LOST
	detectedAt time.Time

	// เวลาเริ่มที่ signal กลับมา
	recoverySince time.Time
}

func NewSignalLossDetector(
	config Config,
) (*SignalLossDetector, error) {
	if config.SignalLossDuration <= 0 {
		return nil, fmt.Errorf(
			"signal loss duration must be greater than 0",
		)
	}

	if config.RecoveryDuration <= 0 {
		return nil, fmt.Errorf(
			"recovery duration must be greater than 0",
		)
	}

	return &SignalLossDetector{
		config: config,
		state:  StateHealthy,
	}, nil
}

func (d *SignalLossDetector) Process(
	sample audio.Sample,
) Result {
	result := Result{
		State: d.state,
	}

	// --------------------------------------------------
	// Mute เป็นอีก diagnostic หนึ่ง
	//
	// ตอนนี้ SignalLossDetector จะไม่เอา mute
	// มานับว่าเป็น input signal failure
	// --------------------------------------------------

	if sample.Muted {
		d.recoverySince = time.Time{}

		if d.state == StateHealthy {
			d.lossSince = time.Time{}
		}

		return result
	}

	// --------------------------------------------------
	// ไม่มี signal
	// --------------------------------------------------

	if !sample.SignalPresent {
		return d.processSignalMissing(sample)
	}

	// --------------------------------------------------
	// มี signal
	// --------------------------------------------------

	return d.processSignalPresent(sample)
}

func (d *SignalLossDetector) processSignalMissing(
	sample audio.Sample,
) Result {
	// ถ้า Confirm ว่า SIGNAL_LOST อยู่แล้ว
	// แค่ reset recovery timer
	if d.state == StateSignalLost {
		d.recoverySince = time.Time{}

		return Result{
			State: d.state,
		}
	}

	// เจอ no-signal ครั้งแรก
	if d.lossSince.IsZero() {
		d.lossSince = sample.Timestamp
	}

	lossFor := sample.Timestamp.Sub(
		d.lossSince,
	)

	// ยังไม่ครบ 10s
	if lossFor < d.config.SignalLossDuration {
		return Result{
			State: d.state,
		}
	}

	// ----------------------------------------------
	// HEALTHY → SIGNAL_LOST
	// ----------------------------------------------

	d.state = StateSignalLost
	d.detectedAt = sample.Timestamp
	d.recoverySince = time.Time{}

	return Result{
		State:        d.state,
		Transition:   TransitionSignalLost,
		LossDuration: lossFor,
	}
}

func (d *SignalLossDetector) processSignalPresent(
	sample audio.Sample,
) Result {
	// --------------------------------------------------
	// ถ้ายัง HEALTHY อยู่
	// signal กลับมาแปลว่า candidate loss ก่อนหน้านี้ไม่จริง
	// --------------------------------------------------

	if d.state == StateHealthy {
		d.lossSince = time.Time{}
		d.detectedAt = time.Time{}
		d.recoverySince = time.Time{}

		return Result{
			State: d.state,
		}
	}

	// --------------------------------------------------
	// ตอนนี้อยู่ SIGNAL_LOST
	// และเพิ่งเริ่มเจอ Signal กลับมา
	// --------------------------------------------------

	if d.recoverySince.IsZero() {
		d.recoverySince = sample.Timestamp

		return Result{
			State: d.state,
		}
	}

	recoveryFor := sample.Timestamp.Sub(
		d.recoverySince,
	)

	// ยังไม่ครบ recovery 5s
	if recoveryFor < d.config.RecoveryDuration {
		return Result{
			State: d.state,
		}
	}

	// --------------------------------------------------
	// SIGNAL_LOST → HEALTHY
	// --------------------------------------------------

	totalLossDuration := sample.Timestamp.Sub(
		d.lossSince,
	)

	incidentDuration := sample.Timestamp.Sub(
		d.detectedAt,
	)

	d.state = StateHealthy

	d.lossSince = time.Time{}
	d.detectedAt = time.Time{}
	d.recoverySince = time.Time{}

	return Result{
		State:            d.state,
		Transition:       TransitionRecovered,
		LossDuration:     totalLossDuration,
		IncidentDuration: incidentDuration,
	}
}

func (d *SignalLossDetector) State() State {
	return d.state
}
