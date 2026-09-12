package obs

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/andreykaipov/goobs/api/events"
	"github.com/andreykaipov/goobs/api/typedefs"

	"live-monitor/internal/agent/audio"
)

type TrackRoutingEvent struct {
	InputName   string
	InputTracks []int
	Timestamp   time.Time
}

func (c *Client) DispatchEvents(
	ctx context.Context,
	inputName string,
	audioOut chan<- audio.Sample,
	routingOut chan<- TrackRoutingEvent,
) error {
	// --------------------------------------------------
	// Initial mute state
	// --------------------------------------------------

	currentMuted, err := c.GetMuteState(inputName)
	if err != nil {
		return err
	}

	// --------------------------------------------------
	// Important:
	// This is now the ONLY place that reads
	// c.raw.IncomingEvents.
	// --------------------------------------------------

	for {
		select {

		case <-ctx.Done():
			return nil

		case event, ok := <-c.raw.IncomingEvents:
			if !ok {
				return errors.New(
					"OBS event stream closed",
				)
			}

			switch event := event.(type) {

			// ==========================================
			// Mute state changed
			// ==========================================

			case *events.InputMuteStateChanged:
				if event.InputName != inputName {
					continue
				}

				currentMuted = event.InputMuted

			// ==========================================
			// Audio meter
			// ==========================================

			case *events.InputVolumeMeters:
				for _, input := range event.Inputs {
					if input.Name != inputName {
						continue
					}

					levelDB, signalPresent := peakLevel(
						input.Levels,
					)

					sample := audio.Sample{
						InputName:     inputName,
						LevelDB:       levelDB,
						SignalPresent: signalPresent,
						Muted:         currentMuted,
						Timestamp:     time.Now(),
					}

					select {
					case audioOut <- sample:

					case <-ctx.Done():
						return nil
					}
				}

			// ==========================================
			// Audio Track assignment changed
			// ==========================================

			case *events.InputAudioTracksChanged:
				if event.InputName != inputName {
					continue
				}

				update := TrackRoutingEvent{
					InputName: inputName,
					InputTracks: enabledAudioTracks(
						event.InputAudioTracks,
					),
					Timestamp: time.Now(),
				}

				select {
				case routingOut <- update:

				case <-ctx.Done():
					return nil
				}
			}
		}
	}
}

func enabledAudioTracks(
	tracks *typedefs.InputAudioTracks,
) []int {
	if tracks == nil {
		return nil
	}

	enabled := make(
		[]int,
		0,
		6,
	)

	for track := 1; track <= 6; track++ {
		key := strconv.Itoa(track)

		if (*tracks)[key] {
			enabled = append(
				enabled,
				track,
			)
		}
	}

	return enabled
}
