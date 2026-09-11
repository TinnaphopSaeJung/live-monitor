package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"live-monitor/internal/agent/audio"
	"live-monitor/internal/agent/detector"
	obsclient "live-monitor/internal/agent/obs"
	"live-monitor/internal/config"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	// --------------------------------------------------
	// 1. Command-line flags
	// --------------------------------------------------

	configPath := flag.String(
		"config",
		"configs/agent.yaml",
		"path to agent config file",
	)

	flag.Parse()

	// --------------------------------------------------
	// 2. Load config
	// --------------------------------------------------

	cfg, err := config.LoadAgent(*configPath)
	if err != nil {
		return fmt.Errorf(
			"load config: %w",
			err,
		)
	}

	// --------------------------------------------------
	// 3. Parse durations
	// --------------------------------------------------

	signalLossDuration, err := time.ParseDuration(
		cfg.Audio.SignalLossDuration,
	)
	if err != nil {
		return fmt.Errorf(
			"parse signal loss duration: %w",
			err,
		)
	}

	lowLevelDuration, err := time.ParseDuration(
		cfg.Audio.LowLevelDuration,
	)
	if err != nil {
		return fmt.Errorf(
			"parse low level duration: %w",
			err,
		)
	}

	lowLevelWindowDuration, err := time.ParseDuration(
		cfg.Audio.LowLevelWindowDuration,
	)
	if err != nil {
		return fmt.Errorf(
			"parse low level window duration: %w",
			err,
		)
	}

	recoveryDuration, err := time.ParseDuration(
		cfg.Audio.RecoveryDuration,
	)
	if err != nil {
		return fmt.Errorf(
			"parse recovery duration: %w",
			err,
		)
	}

	// --------------------------------------------------
	// 4. Create Signal Loss Detector
	// --------------------------------------------------

	signalLossDetector, err := detector.NewSignalLossDetector(
		detector.Config{
			SignalLossDuration: signalLossDuration,
			RecoveryDuration:   recoveryDuration,
		},
	)
	if err != nil {
		return fmt.Errorf(
			"create signal loss detector: %w",
			err,
		)
	}

	// --------------------------------------------------
	// 5. Create Low Level Detector
	// --------------------------------------------------

	lowLevelDetector, err := detector.NewLowLevelDetector(
		detector.LowLevelConfig{
			ThresholdDB:      cfg.Audio.LowLevelThresholdDB,
			LowLevelDuration: lowLevelDuration,
			RecoveryDuration: recoveryDuration,
		},
	)
	if err != nil {
		return fmt.Errorf(
			"create low level detector: %w",
			err,
		)
	}

	// --------------------------------------------------
	// 6. Connect OBS
	// --------------------------------------------------

	client, err := obsclient.New(
		cfg.OBS.Host,
		cfg.OBS.Port,
		cfg.OBS.Password,
	)
	if err != nil {
		return fmt.Errorf(
			"connect OBS: %w",
			err,
		)
	}
	defer client.Close()

	printLog("Connected to OBS")

	// --------------------------------------------------
	// 7. Ensure target input exists
	// --------------------------------------------------

	if err := client.EnsureInput(
		cfg.OBS.AudioInput,
	); err != nil {
		return err
	}

	printLog(
		"Input found: %s",
		cfg.OBS.AudioInput,
	)

	// --------------------------------------------------
	// 8. Graceful shutdown
	// --------------------------------------------------

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
	)
	defer stop()

	// --------------------------------------------------
	// 9. Audio Sample Pipeline
	// --------------------------------------------------

	samples := make(
		chan audio.Sample,
		64,
	)

	streamErr := make(
		chan error,
		1,
	)

	go func() {
		streamErr <- client.StreamAudioSamples(
			ctx,
			cfg.OBS.AudioInput,
			samples,
		)
	}()

	printLog(
		"Audio sample stream started",
	)

	printLog(
		"Signal loss detector loss=%s recovery=%s",
		signalLossDuration,
		recoveryDuration,
	)

	printLog(
		"Low level detector threshold=%.1f dB duration=%s window=%s recovery=%s",
		cfg.Audio.LowLevelThresholdDB,
		lowLevelDuration,
		lowLevelWindowDuration,
		recoveryDuration,
	)

	// --------------------------------------------------
	// 10. Low Level Window
	// --------------------------------------------------

	windowAggregator := audio.NewLevelWindowAggregator(
		time.Now(),
	)

	windowTicker := time.NewTicker(
		lowLevelWindowDuration,
	)
	defer windowTicker.Stop()

	var latestSample audio.Sample

	// --------------------------------------------------
	// 11. Main Event Loop
	// --------------------------------------------------

	for {
		select {

		// ==============================================
		// Shutdown
		// ==============================================

		case <-ctx.Done():
			printLog("Agent stopped")
			return nil

		// ==============================================
		// OBS Audio Stream Error
		// ==============================================

		case err := <-streamErr:
			if err != nil {
				return fmt.Errorf(
					"audio sample stream stopped: %w",
					err,
				)
			}

			printLog("Agent stopped")
			return nil

		// ==============================================
		// Raw Audio Sample
		// ==============================================

		case sample := <-samples:
			latestSample = sample

			// ------------------------------------------
			// Signal Loss Detector
			//
			// ยังคงใช้ Raw Sample ~50ms
			// ------------------------------------------

			signalResult := signalLossDetector.Process(
				sample,
			)

			switch signalResult.Transition {

			case detector.TransitionSignalLost:
				printLog(
					"AUDIO SIGNAL LOST level=%.1f dB no_signal_for=%s",
					sample.LevelDB,
					signalResult.LossDuration.Round(
						time.Millisecond,
					),
				)

			case detector.TransitionRecovered:
				printLog(
					"AUDIO SIGNAL RECOVERED level=%.1f dB incident_for=%s total_loss=%s",
					sample.LevelDB,
					signalResult.IncidentDuration.Round(
						time.Millisecond,
					),
					signalResult.LossDuration.Round(
						time.Millisecond,
					),
				)
			}

			// ------------------------------------------
			// ส่ง Raw Sample เข้า Window Aggregator
			// ------------------------------------------

			windowAggregator.Add(
				sample,
			)

		// ==============================================
		// 1-second Level Window complete
		// ==============================================

		case tickAt := <-windowTicker.C:
			window := windowAggregator.Flush(
				tickAt,
			)

			// ------------------------------------------
			// Low Level Detector
			//
			// ตอนนี้รับ 1 Window / second
			// ไม่ได้รับ raw 50ms sample แล้ว
			// ------------------------------------------

			lowLevelResult := lowLevelDetector.Process(
				window,
			)

			switch lowLevelResult.Transition {

			case detector.LowLevelTransitionTooLow:
				printLog(
					"AUDIO LEVEL TOO LOW level=%.1f dB threshold=%.1f dB low_for=%s",
					window.MaxDB,
					cfg.Audio.LowLevelThresholdDB,
					lowLevelResult.LowLevelDuration.Round(
						time.Millisecond,
					),
				)

			case detector.LowLevelTransitionRecovered:
				printLog(
					"AUDIO LEVEL RECOVERED level=%.1f dB incident_for=%s total_low=%s",
					window.MaxDB,
					lowLevelResult.IncidentDuration.Round(
						time.Millisecond,
					),
					lowLevelResult.LowLevelDuration.Round(
						time.Millisecond,
					),
				)
			}

			// ------------------------------------------
			// Terminal Output
			//
			// ใช้ Window เดียวกับที่ Detector ใช้
			// ------------------------------------------

			if window.SampleCount == 0 {
				printLog(
					"%s no audio samples signal_state=%s level_state=%s",
					cfg.OBS.AudioInput,
					signalLossDetector.State(),
					lowLevelDetector.State(),
				)

				continue
			}

			if window.UsableSampleCount == 0 {
				printLog(
					"%s no usable level signal=%t muted=%t signal_state=%s level_state=%s",
					cfg.OBS.AudioInput,
					latestSample.SignalPresent,
					latestSample.Muted,
					signalLossDetector.State(),
					lowLevelDetector.State(),
				)

				continue
			}

			printLog(
				"%s level=%.1f dB signal=%t muted=%t signal_state=%s level_state=%s",
				cfg.OBS.AudioInput,
				window.MaxDB,
				latestSample.SignalPresent,
				latestSample.Muted,
				signalLossDetector.State(),
				lowLevelDetector.State(),
			)
		}
	}
}

func printLog(
	format string,
	args ...any,
) {
	message := fmt.Sprintf(
		format,
		args...,
	)

	fmt.Printf(
		"[%s] %s\n",
		time.Now().Format("15:04:05"),
		message,
	)
}
