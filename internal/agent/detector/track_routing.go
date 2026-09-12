package detector

import "fmt"

type TrackRoutingState string

const (
	TrackRoutingStateHealthy TrackRoutingState = "HEALTHY"
	TrackRoutingStateInvalid TrackRoutingState = "ROUTING_INVALID"
)

type TrackRoutingTransition string

const (
	TrackRoutingTransitionNone      TrackRoutingTransition = ""
	TrackRoutingTransitionInvalid   TrackRoutingTransition = "ROUTING_INVALID"
	TrackRoutingTransitionRecovered TrackRoutingTransition = "RECOVERED"
)

type TrackRoutingConfig struct {
	StreamTrack int
}

type TrackRoutingResult struct {
	State      TrackRoutingState
	Transition TrackRoutingTransition

	StreamTrack int
	InputTracks []int
}

type TrackRoutingDetector struct {
	config TrackRoutingConfig
	state  TrackRoutingState
}

func NewTrackRoutingDetector(
	config TrackRoutingConfig,
) (*TrackRoutingDetector, error) {
	if config.StreamTrack < 1 || config.StreamTrack > 6 {
		return nil, fmt.Errorf(
			"stream track must be between 1 and 6, got %d",
			config.StreamTrack,
		)
	}

	return &TrackRoutingDetector{
		config: config,
		state:  TrackRoutingStateHealthy,
	}, nil
}

func (d *TrackRoutingDetector) Process(
	inputTracks []int,
) TrackRoutingResult {
	valid := containsAudioTrack(
		inputTracks,
		d.config.StreamTrack,
	)

	// --------------------------------------------------
	// Routing ถูกต้อง
	// --------------------------------------------------

	if valid {
		// เดิมก็ HEALTHY อยู่แล้ว
		if d.state == TrackRoutingStateHealthy {
			return TrackRoutingResult{
				State:       d.state,
				StreamTrack: d.config.StreamTrack,
				InputTracks: inputTracks,
			}
		}

		// ROUTING_INVALID → HEALTHY
		d.state = TrackRoutingStateHealthy

		return TrackRoutingResult{
			State:       d.state,
			Transition:  TrackRoutingTransitionRecovered,
			StreamTrack: d.config.StreamTrack,
			InputTracks: inputTracks,
		}
	}

	// --------------------------------------------------
	// Routing ไม่ถูกต้อง
	// --------------------------------------------------

	// INVALID อยู่แล้ว
	// ไม่ต้อง emit transition ซ้ำ
	if d.state == TrackRoutingStateInvalid {
		return TrackRoutingResult{
			State:       d.state,
			StreamTrack: d.config.StreamTrack,
			InputTracks: inputTracks,
		}
	}

	// HEALTHY → ROUTING_INVALID
	d.state = TrackRoutingStateInvalid

	return TrackRoutingResult{
		State:       d.state,
		Transition:  TrackRoutingTransitionInvalid,
		StreamTrack: d.config.StreamTrack,
		InputTracks: inputTracks,
	}
}

func (d *TrackRoutingDetector) State() TrackRoutingState {
	return d.state
}

func containsAudioTrack(
	tracks []int,
	target int,
) bool {
	for _, track := range tracks {
		if track == target {
			return true
		}
	}

	return false
}
