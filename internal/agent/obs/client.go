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
)

const meterFloorDB = -100.0

type Client struct {
	raw *goobs.Client
}

type AudioSample struct {
	InputName string
	LevelDB   float64
	Muted     bool
	Timestamp time.Time
}

func New(host string, port int, password string) (*Client, error) {
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

func (c *Client) EnsureInput(inputName string) error {
	response, err := c.raw.Inputs.GetInputList() // ขอ input จาก obs (desktop audio, mic/aux, media source ect.)
	if err != nil {
		return fmt.Errorf("get OBS input list: %w", err)
	}

	availableInputs := make([]string, 0, len(response.Inputs))

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

func (c *Client) GetMuteState(inputName string) (bool, error) {
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

func (c *Client) RunAudioProbe(
	ctx context.Context,
	inputName string,
	printInterval time.Duration,
	onSample func(AudioSample),
) error {
	if printInterval <= 0 {
		printInterval = time.Second
	}

	var lastPrint time.Time

	for {
		select {
		case <-ctx.Done():
			return nil

		case event, ok := <-c.raw.IncomingEvents:
			if !ok {
				return errors.New("OBS event stream closed")
			}

			volumeEvent, ok := event.(*events.InputVolumeMeters)
			if !ok {
				continue
			}

			for _, input := range volumeEvent.Inputs {
				if input.Name != inputName {
					continue
				}

				if !lastPrint.IsZero() && // เราต้องการแค่ 1 ครั้ง / วินาที
					time.Since(lastPrint) < printInterval {
					continue
				}

				levelDB := peakDB(input.Levels) // แปลงค่าเสียงจาก obs เป็น dB

				muted, err := c.GetMuteState(inputName)
				if err != nil {
					return err
				}

				now := time.Now()

				onSample(AudioSample{
					InputName: inputName,
					LevelDB:   levelDB,
					Muted:     muted,
					Timestamp: now,
				})

				lastPrint = now
			}
		}
	}
}

func peakDB(levels [][3]float64) float64 {
	var maxPeak float64

	for _, channel := range levels {
		peak := channel[1]

		if peak > maxPeak {
			maxPeak = peak
		}
	}

	if maxPeak <= 0 {
		return meterFloorDB
	}

	return 20 * math.Log10(maxPeak)
}
