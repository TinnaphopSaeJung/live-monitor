package audio

import "time"

type LevelWindow struct {
	StartAt time.Time
	EndAt   time.Time

	MinDB float64
	MaxDB float64

	// จำนวน raw samples ทั้งหมดที่เข้ามาใน window
	SampleCount int

	// จำนวน samples ที่เหมาะสำหรับ Low Level Detection
	//
	// ไม่นับ:
	// - muted=true
	// - SignalPresent=false
	UsableSampleCount int
}

type LevelWindowAggregator struct {
	startAt time.Time

	minDB float64
	maxDB float64

	sampleCount       int
	usableSampleCount int
}

func NewLevelWindowAggregator(
	startAt time.Time,
) *LevelWindowAggregator {
	return &LevelWindowAggregator{
		startAt: startAt,
	}
}

func (a *LevelWindowAggregator) Add(
	sample Sample,
) {
	a.sampleCount++

	// LowLevelDetector ไม่ควรใช้
	// muted หรือ signal-loss samples
	if sample.Muted || !sample.SignalPresent {
		return
	}

	// Sample ที่ usable ตัวแรกของ window
	if a.usableSampleCount == 0 {
		a.minDB = sample.LevelDB
		a.maxDB = sample.LevelDB
		a.usableSampleCount = 1

		return
	}

	if sample.LevelDB < a.minDB {
		a.minDB = sample.LevelDB
	}

	if sample.LevelDB > a.maxDB {
		a.maxDB = sample.LevelDB
	}

	a.usableSampleCount++
}

func (a *LevelWindowAggregator) Flush(
	endAt time.Time,
) LevelWindow {
	window := LevelWindow{
		StartAt:           a.startAt,
		EndAt:             endAt,
		MinDB:             a.minDB,
		MaxDB:             a.maxDB,
		SampleCount:       a.sampleCount,
		UsableSampleCount: a.usableSampleCount,
	}

	// เริ่ม window ใหม่
	a.startAt = endAt

	a.minDB = 0
	a.maxDB = 0
	a.sampleCount = 0
	a.usableSampleCount = 0

	return window
}
