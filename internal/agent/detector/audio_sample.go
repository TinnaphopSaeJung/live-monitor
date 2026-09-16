package detector

import "time"

type AudioSampleState string

const (
	AudioSampleHealthy AudioSampleState = "HEALTHY"
	AudioSampleStalled AudioSampleState = "STALLED"
)

type AudioSampleTransition string

const (
	AudioSampleNoTransition AudioSampleTransition = ""

	AudioSampleBecameStalled AudioSampleTransition = "STALLED"

	AudioSampleRecovered AudioSampleTransition = "RECOVERED"
)

const recoverySampleGap = 500 * time.Millisecond

type AudioSampleWatchdog struct {
	state AudioSampleState

	stallDuration    time.Duration
	recoveryDuration time.Duration

	monitoring bool

	monitoringStartedAt time.Time
	lastSampleAt        time.Time

	recoveryStartedAt time.Time
}

func NewAudioSampleWatchdog(
	stallDuration time.Duration,
	recoveryDuration time.Duration,
) *AudioSampleWatchdog {
	return &AudioSampleWatchdog{
		state:            AudioSampleHealthy,
		stallDuration:    stallDuration,
		recoveryDuration: recoveryDuration,
	}
}

func (w *AudioSampleWatchdog) State() AudioSampleState {
	return w.state
}

func (w *AudioSampleWatchdog) SetMonitoring(
	active bool,
	now time.Time,
) {
	if active == w.monitoring {
		return
	}

	w.monitoring = active

	if active {
		w.monitoringStartedAt = now
		w.lastSampleAt = time.Time{}
		w.recoveryStartedAt = time.Time{}
		w.state = AudioSampleHealthy

		return
	}

	w.monitoringStartedAt = time.Time{}
	w.lastSampleAt = time.Time{}
	w.recoveryStartedAt = time.Time{}
	w.state = AudioSampleHealthy
}

func (w *AudioSampleWatchdog) ObserveSample(
	now time.Time,
) AudioSampleTransition {
	if !w.monitoring {
		return AudioSampleNoTransition
	}

	previousSampleAt := w.lastSampleAt
	w.lastSampleAt = now

	if w.state != AudioSampleStalled {
		return AudioSampleNoTransition
	}

	if w.recoveryStartedAt.IsZero() {
		w.recoveryStartedAt = now

		return AudioSampleNoTransition
	}

	if !previousSampleAt.IsZero() &&
		now.Sub(previousSampleAt) > recoverySampleGap {

		w.recoveryStartedAt = now

		return AudioSampleNoTransition
	}

	if now.Sub(w.recoveryStartedAt) >=
		w.recoveryDuration {

		w.state = AudioSampleHealthy
		w.recoveryStartedAt = time.Time{}

		return AudioSampleRecovered
	}

	return AudioSampleNoTransition
}

func (w *AudioSampleWatchdog) Check(
	now time.Time,
) AudioSampleTransition {
	if !w.monitoring {
		return AudioSampleNoTransition
	}

	if w.state == AudioSampleStalled {
		return AudioSampleNoTransition
	}

	var referenceTime time.Time

	if !w.lastSampleAt.IsZero() {
		referenceTime = w.lastSampleAt
	} else {
		referenceTime = w.monitoringStartedAt
	}

	if referenceTime.IsZero() {
		return AudioSampleNoTransition
	}

	if now.Sub(referenceTime) <
		w.stallDuration {

		return AudioSampleNoTransition
	}

	w.state = AudioSampleStalled
	w.recoveryStartedAt = time.Time{}

	return AudioSampleBecameStalled
}
