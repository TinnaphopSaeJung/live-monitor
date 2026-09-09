package obs

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/andreykaipov/goobs"
	"github.com/andreykaipov/goobs/api/events"
	"github.com/andreykaipov/goobs/api/events/subscriptions"
	inputrequests "github.com/andreykaipov/goobs/api/requests/inputs"

	"live-monitor/internal/agent/audio"
)

const meterFloorDB = -100.0

type Client struct {
	raw *goobs.Client
}

func New(
	host string,
	port int,
	password string,
) (*Client, error) {
	address := net.JoinHostPort(
		host,
		strconv.Itoa(port),
	)

	client, err := goobs.New(
		address,
		goobs.WithPassword(password),
		goobs.WithEventSubscriptions(
			subscriptions.All|
				subscriptions.InputVolumeMeters,
		),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"connect to OBS at %s: %w",
			address,
			err,
		)
	}

	return &Client{
		raw: client,
	}, nil
}

func (c *Client) Close() error {
	return c.raw.Disconnect()
}

func (c *Client) EnsureInput(
	inputName string,
) error {
	response, err := c.raw.Inputs.GetInputList()
	if err != nil {
		return fmt.Errorf(
			"get OBS input list: %w",
			err,
		)
	}

	availableInputs := make(
		[]string,
		0,
		len(response.Inputs),
	)

	for _, input := range response.Inputs {
		availableInputs = append(
			availableInputs,
			input.InputName,
		)

		if input.InputName == inputName {
			return nil
		}
	}

	sort.Strings(availableInputs)

	return fmt.Errorf(
		"OBS input %q not found; available inputs: %s",
		inputName,
		strings.Join(availableInputs, ", "),
	)
}

func (c *Client) GetMuteState(
	inputName string,
) (bool, error) {
	params := inputrequests.
		NewGetInputMuteParams().
		WithInputName(inputName)

	response, err := c.raw.Inputs.GetInputMute(params)
	if err != nil {
		return false, fmt.Errorf(
			"get mute state for %q: %w",
			inputName,
			err,
		)
	}

	return response.InputMuted, nil
}

func (c *Client) StreamAudioSamples(
	ctx context.Context,
	inputName string,
	out chan<- audio.Sample,
) error {
	currentMuted, err := c.GetMuteState(
		inputName,
	)
	if err != nil {
		return err
	}

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

			// ------------------------------------------
			// Mute state changed
			// ------------------------------------------

			case *events.InputMuteStateChanged:
				if event.InputName != inputName {
					continue
				}

				currentMuted = event.InputMuted

			// ------------------------------------------
			// Audio meter event
			// ------------------------------------------

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

					case out <- sample:

					case <-ctx.Done():
						return nil
					}
				}
			}
		}
	}
}

func peakLevel(
	levels [][3]float64,
) (float64, bool) {
	var maxPeak float64

	for _, channel := range levels {
		peak := channel[1]

		if peak > maxPeak {
			maxPeak = peak
		}
	}

	if maxPeak <= 0 {
		return meterFloorDB, false
	}

	levelDB := 20 * math.Log10(maxPeak)

	return levelDB, true
}
