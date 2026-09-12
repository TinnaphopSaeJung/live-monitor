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
	"live-monitor/internal/agent/monitor"
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

	// ==========================================
	// Mute Detector
	// ==========================================

	muteDetector := detector.NewMuteDetector()

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

	streamStatus, err := client.GetStreamStatus()
	if err != nil {
		return fmt.Errorf(
			"stream state probe: %w",
			err,
		)
	}

	monitoringContext := monitor.NewContext(
		streamStatus.Active,
		streamStatus.Reconnecting,
	)

	printLog(
		"Streaming status active=%t reconnecting=%t state=%s monitoring=%t",
		streamStatus.Active,
		streamStatus.Reconnecting,
		monitoringContext.StreamingState(),
		monitoringContext.MonitoringEnabled(),
	)

	// --------------------------------------------------
	// Track Routing Probe
	// --------------------------------------------------

	routing, err := client.GetTrackRouting(
		cfg.OBS.AudioInput,
	)
	if err != nil {
		return fmt.Errorf(
			"track routing probe: %w",
			err,
		)
	}

	printLog(
		"Track routing input=%s tracks=%v output_mode=%s stream_track=%d valid=%t",
		routing.InputName,
		routing.InputTracks,
		routing.OutputMode,
		routing.StreamTrack,
		routing.Valid,
	)

	trackRoutingDetector, err := detector.NewTrackRoutingDetector(
		detector.TrackRoutingConfig{
			StreamTrack: routing.StreamTrack,
		},
	)
	if err != nil {
		return fmt.Errorf(
			"create track routing detector: %w",
			err,
		)
	}

	initialRoutingResult := trackRoutingDetector.Process(
		routing.InputTracks,
	)

	if initialRoutingResult.Transition ==
		detector.TrackRoutingTransitionInvalid {

		printLog(
			"AUDIO ROUTING INVALID stream_track=%d input_tracks=%v",
			initialRoutingResult.StreamTrack,
			initialRoutingResult.InputTracks,
		)
	}

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

	routingEvents := make(
		chan obsclient.TrackRoutingEvent,
		8,
	)

	streamEvents := make(
		chan obsclient.StreamStateEvent,
		8,
	)

	eventErr := make(
		chan error,
		1,
	)

	go func() {
		eventErr <- client.DispatchEvents(
			ctx,
			cfg.OBS.AudioInput,
			samples,
			routingEvents,
			streamEvents,
		)
	}()

	printLog(
		"OBS event dispatcher started",
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

		case err := <-eventErr:
			if err != nil {
				return fmt.Errorf(
					"OBS event dispatcher stopped: %w",
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

			// ==========================================
			// Mute Detector
			// ==========================================

			muteResult := muteDetector.Process(
				sample,
			)

			switch muteResult.Transition {

			case detector.MuteTransitionMuted:
				printLog(
					"AUDIO MUTED",
				)

			case detector.MuteTransitionUnmuted:
				printLog(
					"AUDIO UNMUTED muted_for=%s",
					muteResult.MutedDuration.Round(
						time.Millisecond,
					),
				)
			}

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

		case routingEvent := <-routingEvents:
			routingResult := trackRoutingDetector.Process(
				routingEvent.InputTracks,
			)

			switch routingResult.Transition {

			case detector.TrackRoutingTransitionInvalid:
				printLog(
					"AUDIO ROUTING INVALID stream_track=%d input_tracks=%v",
					routingResult.StreamTrack,
					routingResult.InputTracks,
				)

			case detector.TrackRoutingTransitionRecovered:
				printLog(
					"AUDIO ROUTING RECOVERED stream_track=%d input_tracks=%v",
					routingResult.StreamTrack,
					routingResult.InputTracks,
				)
			}

		case streamEvent := <-streamEvents:
			update := monitoringContext.ApplyStreamEvent(
				streamEvent.Active,
				streamEvent.State,
			)

			printLog(
				"STREAM STATE CHANGED active=%t obs_state=%s state=%s monitoring=%t",
				streamEvent.Active,
				streamEvent.State,
				update.CurrentState,
				monitoringContext.MonitoringEnabled(),
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
					"%s no audio samples signal_state=%s level_state=%s mute_state=%s routing_state=%s stream_state=%s",
					cfg.OBS.AudioInput,
					signalLossDetector.State(),
					lowLevelDetector.State(),
					muteDetector.State(),
					trackRoutingDetector.State(),
					monitoringContext.StreamingState(),
				)

				continue
			}

			if window.UsableSampleCount == 0 {
				printLog(
					"%s no usable level signal=%t muted=%t signal_state=%s level_state=%s mute_state=%s routing_state=%s stream_state=%s",
					cfg.OBS.AudioInput,
					latestSample.SignalPresent,
					latestSample.Muted,
					signalLossDetector.State(),
					lowLevelDetector.State(),
					muteDetector.State(),
					trackRoutingDetector.State(),
					monitoringContext.StreamingState(),
				)

				continue
			}

			printLog(
				"%s level=%.1f dB signal=%t muted=%t signal_state=%s level_state=%s mute_state=%s routing_state=%s stream_state=%s",
				cfg.OBS.AudioInput,
				window.MaxDB,
				latestSample.SignalPresent,
				latestSample.Muted,
				signalLossDetector.State(),
				lowLevelDetector.State(),
				muteDetector.State(),
				trackRoutingDetector.State(),
				monitoringContext.StreamingState(),
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
