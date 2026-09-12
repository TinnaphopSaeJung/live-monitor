package obs

import (
	"fmt"
	"strconv"
	"strings"

	configrequests "github.com/andreykaipov/goobs/api/requests/config"
	inputrequests "github.com/andreykaipov/goobs/api/requests/inputs"
)

type TrackRouting struct {
	InputName string

	// Tracks ที่ Mic/Aux ถูก assign อยู่
	InputTracks []int

	// Simple / Advanced
	OutputMode string

	// Track ที่ Streaming Output ใช้จริง
	StreamTrack int

	// Input ถูกส่งเข้า StreamTrack หรือไม่
	Valid bool
}

func (c *Client) GetTrackRouting(
	inputName string,
) (TrackRouting, error) {
	inputTracks, err := c.getInputAudioTracks(
		inputName,
	)
	if err != nil {
		return TrackRouting{}, err
	}

	outputMode, err := c.getOutputMode()
	if err != nil {
		return TrackRouting{}, err
	}

	streamTrack, err := c.getStreamTrack(
		outputMode,
	)
	if err != nil {
		return TrackRouting{}, err
	}

	return TrackRouting{
		InputName:   inputName,
		InputTracks: inputTracks,
		OutputMode:  outputMode,
		StreamTrack: streamTrack,
		Valid:       containsTrack(inputTracks, streamTrack),
	}, nil
}

func (c *Client) getInputAudioTracks(
	inputName string,
) ([]int, error) {
	params := inputrequests.
		NewGetInputAudioTracksParams().
		WithInputName(inputName)

	response, err := c.raw.Inputs.GetInputAudioTracks(
		params,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"get input audio tracks for %q: %w",
			inputName,
			err,
		)
	}

	if response.InputAudioTracks == nil {
		return nil, fmt.Errorf(
			"input audio tracks for %q are empty",
			inputName,
		)
	}

	tracks := make([]int, 0, 6)

	for track := 1; track <= 6; track++ {
		key := strconv.Itoa(track)

		enabled, ok := (*response.InputAudioTracks)[key]
		if !ok {
			continue
		}

		if enabled {
			tracks = append(
				tracks,
				track,
			)
		}
	}

	return tracks, nil
}

func (c *Client) getOutputMode() (
	string,
	error,
) {
	value, err := c.getProfileParameter(
		"Output",
		"Mode",
	)
	if err != nil {
		return "", fmt.Errorf(
			"get output mode: %w",
			err,
		)
	}

	if value == "" {
		return "", fmt.Errorf(
			"OBS output mode is empty",
		)
	}

	return value, nil
}

func (c *Client) getStreamTrack(
	outputMode string,
) (int, error) {
	switch strings.ToLower(outputMode) {

	case "simple":
		// OBS Simple Output streaming uses Track 1.
		return 1, nil

	case "advanced":
		value, err := c.getProfileParameter(
			"AdvOut",
			"TrackIndex",
		)
		if err != nil {
			return 0, fmt.Errorf(
				"get advanced stream track: %w",
				err,
			)
		}

		track, err := strconv.Atoi(value)
		if err != nil {
			return 0, fmt.Errorf(
				"invalid AdvOut.TrackIndex %q: %w",
				value,
				err,
			)
		}

		if track < 1 || track > 6 {
			return 0, fmt.Errorf(
				"stream track must be between 1 and 6, got %d",
				track,
			)
		}

		return track, nil

	default:
		return 0, fmt.Errorf(
			"unsupported OBS output mode %q",
			outputMode,
		)
	}
}

func (c *Client) getProfileParameter(
	category string,
	name string,
) (string, error) {
	params := configrequests.
		NewGetProfileParameterParams().
		WithParameterCategory(category).
		WithParameterName(name)

	response, err := c.raw.Config.GetProfileParameter(
		params,
	)
	if err != nil {
		return "", fmt.Errorf(
			"get profile parameter %s.%s: %w",
			category,
			name,
			err,
		)
	}

	return response.ParameterValue, nil
}

func containsTrack(
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
