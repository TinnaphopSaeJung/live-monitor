package audio

import "time"

type Sample struct {
	InputName string
	LevelDB   float64
	Muted     bool
	Timestamp time.Time
}
