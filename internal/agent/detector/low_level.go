package detector

import (
	"fmt"
	"time"

	"live-monitor/internal/agent/audio"
)

type LowLevelState string

const (
	LowLevelStateHealthy LowLevelState = "HEALTHY"
	LowLevelStateTooLow  LowLevelState = "AUDIO_TOO_LOW"
)

type LowLevelTransition string

const (
	LowLevelTransitionNone      LowLevelTransition = ""
	LowLevelTransitionTooLow    LowLevelTransition = "AUDIO_TOO_LOW"
	LowLevelTransitionRecovered LowLevelTransition = "RECOVERED"
)

type LowLevelConfig struct {
	ThresholdDB      float64
	LowLevelDuration time.Duration
	RecoveryDuration time.Duration
}

type LowLevelResult struct {
	State      LowLevelState
	Transition LowLevelTransition

	LowLevelDuration time.Duration
	IncidentDuration time.Duration
}

type LowLevelDetector struct {
	config LowLevelConfig

	state LowLevelState

	lowSince      time.Time
	detectedAt    time.Time
	recoverySince time.Time
}

func NewLowLevelDetector(
	config LowLevelConfig,
) (*LowLevelDetector, error) {
	if config.ThresholdDB >= 0 {
		return nil, fmt.Errorf(
			"low level threshold must be below 0 dB",
		)
	}

	if config.LowLevelDuration <= 0 {
		return nil, fmt.Errorf(
			"low level duration must be greater than 0",
		)
	}

	if config.RecoveryDuration <= 0 {
		return nil, fmt.Errorf(
			"recovery duration must be greater than 0",
		)
	}

	return &LowLevelDetector{
		config: config,
		state:  LowLevelStateHealthy,
	}, nil
}

func (d *LowLevelDetector) Process(
	window audio.LevelWindow,
) LowLevelResult {
	result := LowLevelResult{
		State: d.state,
	}

	// --------------------------------------------------
	// ไม่มี usable audio samples ใน window นี้
	//
	// เช่น:
	// - muted
	// - signal lost
	//
	// LowLevelDetector ไม่รับผิดชอบกรณีเหล่านี้
	// --------------------------------------------------

	if window.UsableSampleCount == 0 {
		d.recoverySince = time.Time{}

		if d.state == LowLevelStateHealthy {
			d.lowSince = time.Time{}
		}

		return result
	}

	// --------------------------------------------------
	// ถ้า Max ของทั้ง window ยัง <= threshold
	//
	// แปลว่าใน 1 วินาทีนี้ไม่มี sample ไหน
	// ขึ้นสูงกว่า threshold เลย
	// --------------------------------------------------

	if window.MaxDB <= d.config.ThresholdDB {
		return d.processLowWindow(window)
	}

	return d.processNormalWindow(window)
}

func (d *LowLevelDetector) processLowWindow(
	window audio.LevelWindow,
) LowLevelResult {
	// ----------------------------------------------
	// Confirm AUDIO_TOO_LOW อยู่แล้ว
	//
	// เจอ LOW window อีก
	// → recovery ถูกยกเลิก
	// ----------------------------------------------

	if d.state == LowLevelStateTooLow {
		d.recoverySince = time.Time{}

		return LowLevelResult{
			State: d.state,
		}
	}

	// ----------------------------------------------
	// LOW window แรก
	// ----------------------------------------------

	if d.lowSince.IsZero() {
		d.lowSince = window.StartAt
	}

	lowFor := window.EndAt.Sub(
		d.lowSince,
	)

	if lowFor < d.config.LowLevelDuration {
		return LowLevelResult{
			State: d.state,
		}
	}

	// ----------------------------------------------
	// HEALTHY → AUDIO_TOO_LOW
	// ----------------------------------------------

	d.state = LowLevelStateTooLow
	d.detectedAt = window.EndAt
	d.recoverySince = time.Time{}

	return LowLevelResult{
		State:            d.state,
		Transition:       LowLevelTransitionTooLow,
		LowLevelDuration: lowFor,
	}
}

func (d *LowLevelDetector) processNormalWindow(
	window audio.LevelWindow,
) LowLevelResult {
	// ----------------------------------------------
	// ถ้ายัง HEALTHY
	//
	// candidate low-level ก่อนหน้านี้ไม่ต่อเนื่อง
	// ----------------------------------------------

	if d.state == LowLevelStateHealthy {
		d.lowSince = time.Time{}
		d.recoverySince = time.Time{}

		return LowLevelResult{
			State: d.state,
		}
	}

	// ----------------------------------------------
	// ตอนนี้ AUDIO_TOO_LOW
	//
	// เจอ NORMAL window แรก
	// ----------------------------------------------

	if d.recoverySince.IsZero() {
		d.recoverySince = window.StartAt
	}

	recoveryFor := window.EndAt.Sub(
		d.recoverySince,
	)

	if recoveryFor < d.config.RecoveryDuration {
		return LowLevelResult{
			State: d.state,
		}
	}

	// ----------------------------------------------
	// AUDIO_TOO_LOW → HEALTHY
	// ----------------------------------------------

	totalLowDuration := window.EndAt.Sub(
		d.lowSince,
	)

	incidentDuration := window.EndAt.Sub(
		d.detectedAt,
	)

	d.state = LowLevelStateHealthy

	d.lowSince = time.Time{}
	d.detectedAt = time.Time{}
	d.recoverySince = time.Time{}

	return LowLevelResult{
		State:            d.state,
		Transition:       LowLevelTransitionRecovered,
		LowLevelDuration: totalLowDuration,
		IncidentDuration: incidentDuration,
	}
}

func (d *LowLevelDetector) State() LowLevelState {
	return d.state
}
