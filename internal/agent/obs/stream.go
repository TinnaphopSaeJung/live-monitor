package obs

import "fmt"

type StreamStatus struct {
	Active       bool
	Reconnecting bool
	DurationMS   float64
	Timecode     string
}

func (c *Client) GetStreamStatus() (StreamStatus, error) {
	response, err := c.raw.Stream.GetStreamStatus()
	if err != nil {
		return StreamStatus{}, fmt.Errorf(
			"get stream status: %w",
			err,
		)
	}

	return StreamStatus{
		Active:       response.OutputActive,
		Reconnecting: response.OutputReconnecting,
		DurationMS:   response.OutputDuration,
		Timecode:     response.OutputTimecode,
	}, nil
}
