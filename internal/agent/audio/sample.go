package audio

import "time"

type Sample struct {
	InputName     string
	LevelDB       float64
	SignalPresent bool
	Muted         bool
	Timestamp     time.Time
}
