package monitor

type StreamingState string

const (
	StreamingStateUnknown      StreamingState = "UNKNOWN"
	StreamingStateStopped      StreamingState = "STOPPED"
	StreamingStateStarting     StreamingState = "STARTING"
	StreamingStateStreaming    StreamingState = "STREAMING"
	StreamingStateStopping     StreamingState = "STOPPING"
	StreamingStateReconnecting StreamingState = "RECONNECTING"
)

const (
	obsOutputStarting     = "OBS_WEBSOCKET_OUTPUT_STARTING"
	obsOutputStarted      = "OBS_WEBSOCKET_OUTPUT_STARTED"
	obsOutputStopping     = "OBS_WEBSOCKET_OUTPUT_STOPPING"
	obsOutputStopped      = "OBS_WEBSOCKET_OUTPUT_STOPPED"
	obsOutputReconnecting = "OBS_WEBSOCKET_OUTPUT_RECONNECTING"
	obsOutputReconnected  = "OBS_WEBSOCKET_OUTPUT_RECONNECTED"
)

type Context struct {
	streamingState StreamingState
	streamActive   bool
}

type StreamUpdate struct {
	PreviousState StreamingState
	CurrentState  StreamingState

	Active  bool
	Changed bool
}

func NewContext(
	streamActive bool,
	reconnecting bool,
) *Context {
	state := StreamingStateStopped

	if reconnecting {
		state = StreamingStateReconnecting
	} else if streamActive {
		state = StreamingStateStreaming
	}

	return &Context{
		streamingState: state,
		streamActive:   streamActive,
	}
}

func (c *Context) ApplyStreamEvent(
	active bool,
	outputState string,
) StreamUpdate {
	previousState := c.streamingState

	nextState := streamingStateFromOBS(
		outputState,
	)

	c.streamingState = nextState
	c.streamActive = active

	return StreamUpdate{
		PreviousState: previousState,
		CurrentState:  nextState,
		Active:        active,
		Changed:       previousState != nextState,
	}
}

func (c *Context) StreamingState() StreamingState {
	return c.streamingState
}

func (c *Context) StreamActive() bool {
	return c.streamActive
}

// MonitoringEnabled หมายถึง:
// ตอนนี้เป็นช่วงที่ Audio Incident สามารถมีผลกับ Live จริง
//
// รอบแรกเราเปิดเฉพาะ STREAMING เท่านั้น
// STARTING / STOPPING / STOPPED / RECONNECTING
// ยังไม่ถือว่าเปิด Audio Incident
func (c *Context) MonitoringEnabled() bool {
	return c.streamingState == StreamingStateStreaming
}

func streamingStateFromOBS(
	outputState string,
) StreamingState {
	switch outputState {

	case obsOutputStarting:
		return StreamingStateStarting

	case obsOutputStarted:
		return StreamingStateStreaming

	case obsOutputStopping:
		return StreamingStateStopping

	case obsOutputStopped:
		return StreamingStateStopped

	case obsOutputReconnecting:
		return StreamingStateReconnecting

	case obsOutputReconnected:
		return StreamingStateStreaming

	default:
		return StreamingStateUnknown
	}
}
