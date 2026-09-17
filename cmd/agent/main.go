package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"live-monitor/internal/agent/audio"
	"live-monitor/internal/agent/detector"
	"live-monitor/internal/agent/incident"
	"live-monitor/internal/agent/monitor"
	obsclient "live-monitor/internal/agent/obs"
	"live-monitor/internal/agent/reporter"
	"live-monitor/internal/config"
	"live-monitor/internal/contracts"
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

	heartbeatInterval, err := time.ParseDuration(
		cfg.Agent.HeartbeatInterval,
	)
	if err != nil {
		return fmt.Errorf(
			"parse heartbeat interval: %w",
			err,
		)
	}

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
	// Audio Sample Watchdog durations
	// --------------------------------------------------

	sampleStallDuration, err := time.ParseDuration(
		cfg.Audio.SampleStallDuration,
	)
	if err != nil {
		return fmt.Errorf(
			"parse sample stall duration: %w",
			err,
		)
	}

	sampleRecoveryDuration, err := time.ParseDuration(
		cfg.Audio.SampleRecoveryDuration,
	)
	if err != nil {
		return fmt.Errorf(
			"parse sample recovery duration: %w",
			err,
		)
	}

	var backendRequestTimeout time.Duration

	if cfg.Backend.Enabled {
		backendRequestTimeout, err = time.ParseDuration(
			cfg.Backend.RequestTimeout,
		)
		if err != nil {
			return fmt.Errorf(
				"parse backend request timeout: %w",
				err,
			)
		}
	}

	// --------------------------------------------------
	// 4. Create detectors
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

	muteDetector := detector.NewMuteDetector()

	// --------------------------------------------------
	// Audio Sample Watchdog
	//
	// หน้าที่:
	// ตรวจว่า target audio.Sample
	// ยังไหลเข้ามาตามปกติหรือไม่
	// --------------------------------------------------

	sampleWatchdog :=
		detector.NewAudioSampleWatchdog(
			sampleStallDuration,
			sampleRecoveryDuration,
		)

	// --------------------------------------------------
	// 5. Connect OBS
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
	// 6. Ensure audio input exists
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
	// 7. Initial Streaming State
	// --------------------------------------------------

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
	// Sync initial monitoring state เข้า Watchdog
	//
	// สำคัญสำหรับ case:
	//
	// Agent start
	// Streaming=true
	// แต่ไม่เคยได้ audio sample เลย
	// --------------------------------------------------

	sampleWatchdog.SetMonitoring(
		monitoringContext.MonitoringEnabled(),
		time.Now(),
	)

	// --------------------------------------------------
	// 8. Initial Track Routing
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

	// --------------------------------------------------
	// 9. Track Routing Detector
	// --------------------------------------------------

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
	// 10. Graceful shutdown context
	// --------------------------------------------------

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
	)
	defer stop()

	// --------------------------------------------------
	// 11. Incident Manager
	// --------------------------------------------------

	incidentManager := incident.NewManager()

	// --------------------------------------------------
	// 12. Reporter
	// --------------------------------------------------

	var agentReporter reporter.Reporter
	var reporterErrors <-chan error

	if cfg.Backend.Enabled {
		httpReporter := reporter.NewHTTPReporter(
			cfg.Backend.BaseURL,
			cfg.Backend.AgentToken,
			backendRequestTimeout,
		)

		asyncReporter := reporter.NewAsyncReporter(
			httpReporter,
			reporter.AsyncConfig{
				IncidentQueueSize: 64,

				IncidentRetryAttempts: 8,

				RetryInitialBackoff: 500 *
					time.Millisecond,

				RetryMaxBackoff: 5 *
					time.Second,
			},
		)

		agentReporter = asyncReporter
		reporterErrors = asyncReporter.Errors()

		go asyncReporter.Run(
			ctx,
		)

		printLog(
			"Reporter mode=ASYNC_HTTP backend=%s",
			cfg.Backend.BaseURL,
		)

	} else {
		agentReporter = reporter.NewLogReporter()

		printLog(
			"Reporter mode=LOG",
		)
	}

	// --------------------------------------------------
	// 13. Current Health Snapshot
	//
	// NOTE:
	// SampleWatchdog ยังไม่ถูกใส่เข้า IncidentManager
	// ใน Step นี้
	// --------------------------------------------------

	currentHealth := func() incident.HealthSnapshot {
		return incident.HealthSnapshot{
			AudioTooLow: lowLevelDetector.State() ==
				detector.LowLevelStateTooLow,

			Muted: muteDetector.State() ==
				detector.MuteStateMuted,

			SignalLost: signalLossDetector.State() ==
				detector.StateSignalLost,

			RoutingInvalid: trackRoutingDetector.State() ==
				detector.TrackRoutingStateInvalid,

			SampleStalled: sampleWatchdog.State() ==
				detector.AudioSampleStalled,
		}
	}

	// --------------------------------------------------
	// 14. Build Heartbeat
	//
	// NOTE:
	// SampleState ยังไม่ถูกส่ง Backend ใน Step นี้
	// --------------------------------------------------

	buildHeartbeat := func(
		at time.Time,
	) contracts.Heartbeat {
		activeTypes := incidentManager.ActiveTypes()

		activeIncidents := make(
			[]string,
			0,
			len(activeTypes),
		)

		for _, incidentType := range activeTypes {
			activeIncidents = append(
				activeIncidents,
				string(incidentType),
			)
		}

		return contracts.Heartbeat{
			MachineID: cfg.Agent.MachineID,
			SentAt:    at,

			OBSConnected: true,

			StreamState: string(
				monitoringContext.StreamingState(),
			),

			MonitoringActive: monitoringContext.MonitoringEnabled(),

			Audio: contracts.AudioHealth{
				SignalState: string(
					signalLossDetector.State(),
				),

				LevelState: string(
					lowLevelDetector.State(),
				),

				MuteState: string(
					muteDetector.State(),
				),

				RoutingState: string(
					trackRoutingDetector.State(),
				),

				SampleState: string(
					sampleWatchdog.State(),
				),
			},

			ActiveIncidents: activeIncidents,
		}
	}

	buildIncidentEvent := func(
		event incident.Event,
	) contracts.IncidentEvent {
		payload := contracts.IncidentEvent{
			EventID: buildIncidentEventID(
				cfg.Agent.MachineID,
				event,
			),

			MachineID: cfg.Agent.MachineID,

			EventType: string(
				event.EventType,
			),

			IncidentType: string(
				event.IncidentType,
			),

			StartedAt:  event.StartedAt,
			OccurredAt: event.OccurredAt,
		}

		if event.EventType == incident.EventResolved {
			reason := string(
				event.ResolutionReason,
			)

			durationMS := event.Duration.Milliseconds()

			payload.ResolutionReason = &reason
			payload.DurationMS = &durationMS
		}

		return payload
	}

	// --------------------------------------------------
	// 15. Incident Reconciliation
	// --------------------------------------------------

	reconcileIncidents := func(
		at time.Time,
	) {
		events := incidentManager.Reconcile(
			monitoringContext.MonitoringEnabled(),
			currentHealth(),
			at,
		)

		for _, event := range events {
			switch event.EventType {

			case incident.EventOpened:
				printLog(
					"INCIDENT OPENED type=%s",
					event.IncidentType,
				)

			case incident.EventResolved:
				printLog(
					"INCIDENT RESOLVED type=%s reason=%s duration=%s",
					event.IncidentType,
					event.ResolutionReason,
					event.Duration.Round(
						time.Millisecond,
					),
				)
			}

			payload := buildIncidentEvent(
				event,
			)

			if err := agentReporter.SendIncident(
				ctx,
				payload,
			); err != nil {
				printLog(
					"REPORT INCIDENT FAILED: %v",
					err,
				)
			}
		}
	}

	// --------------------------------------------------
	// 16. Reconcile initial state
	// --------------------------------------------------

	reconcileIncidents(
		time.Now(),
	)

	// --------------------------------------------------
	// 17. Event Channels
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

	// --------------------------------------------------
	// 18. OBS Event Dispatcher
	// --------------------------------------------------

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

	printLog(
		"Audio sample watchdog stall=%s recovery=%s",
		sampleStallDuration,
		sampleRecoveryDuration,
	)

	printLog(
		"Heartbeat machine=%s interval=%s",
		cfg.Agent.MachineID,
		heartbeatInterval,
	)

	// --------------------------------------------------
	// 19. Low Level Window Aggregator
	// --------------------------------------------------

	windowAggregator := audio.NewLevelWindowAggregator(
		time.Now(),
	)

	windowTicker := time.NewTicker(
		lowLevelWindowDuration,
	)
	defer windowTicker.Stop()

	// --------------------------------------------------
	// 20. Audio Sample Watchdog Ticker
	//
	// Watchdog ต้องมี ticker แยก
	//
	// เพราะตอน samples หาย
	// case sample := <-samples
	// จะไม่มีวันถูกเรียก
	// --------------------------------------------------

	sampleWatchdogTicker := time.NewTicker(
		250 * time.Millisecond,
	)
	defer sampleWatchdogTicker.Stop()

	// --------------------------------------------------
	// 21. Heartbeat Ticker
	// --------------------------------------------------

	heartbeatTicker := time.NewTicker(
		heartbeatInterval,
	)
	defer heartbeatTicker.Stop()

	// --------------------------------------------------
	// Latest raw sample
	// --------------------------------------------------

	var latestSample audio.Sample

	// --------------------------------------------------
	// 22. Main Event Loop
	// --------------------------------------------------

	for {
		select {

		// ==============================================
		// Shutdown
		// ==============================================

		case <-ctx.Done():
			printLog(
				"Agent stopped",
			)

			return nil

		// ==============================================
		// OBS Event Dispatcher Error
		// ==============================================

		case err := <-eventErr:
			if err != nil {
				return fmt.Errorf(
					"OBS event dispatcher stopped: %w",
					err,
				)
			}

			printLog(
				"Agent stopped",
			)

			return nil

		// ==============================================
		// Raw Audio Sample
		// ==============================================

		case sample := <-samples:
			latestSample = sample

			// ------------------------------------------
			// Audio Sample Watchdog
			//
			// ต้อง Observe ก่อน Detector อื่น
			//
			// Watchdog สนใจเพียง:
			// "มี sample มาถึงหรือไม่"
			//
			// ไม่สนใจ:
			// - muted
			// - signal
			// - dB
			// ------------------------------------------

			sampleTransition :=
				sampleWatchdog.ObserveSample(
					sample.Timestamp,
				)

			switch sampleTransition {

			case detector.AudioSampleRecovered:
				printLog(
					"AUDIO SAMPLES RECOVERED",
				)
			}

			// ------------------------------------------
			// Mute Detector
			// ------------------------------------------

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
			// Mute / Signal states อาจเปลี่ยน
			// ------------------------------------------

			reconcileIncidents(
				sample.Timestamp,
			)

			// ------------------------------------------
			// Add sample to 1-second window
			// ------------------------------------------

			windowAggregator.Add(
				sample,
			)

		// ==============================================
		// Track Routing Changed
		// ==============================================

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

			reconcileIncidents(
				routingEvent.Timestamp,
			)

		// ==============================================
		// Streaming State Changed
		// ==============================================

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

			// ------------------------------------------
			// Sync Monitoring Context → Sample Watchdog
			//
			// STREAMING:
			// เริ่มจับเวลารอ sample
			//
			// STOPPED:
			// reset watchdog
			// ------------------------------------------

			sampleWatchdog.SetMonitoring(
				monitoringContext.MonitoringEnabled(),
				streamEvent.Timestamp,
			)

			// ------------------------------------------
			// ถ้า Detector มีปัญหาตั้งแต่ก่อน Live
			// เมื่อ Stream กลายเป็น STREAMING
			// Incident จะถูกเปิดตรงนี้
			// ------------------------------------------

			reconcileIncidents(
				streamEvent.Timestamp,
			)

			// ==============================================
			// Audio Sample Watchdog Tick
			// ==============================================

		case watchdogAt := <-sampleWatchdogTicker.C:
			transition :=
				sampleWatchdog.Check(
					watchdogAt,
				)

			switch transition {

			case detector.AudioSampleBecameStalled:
				printLog(
					"AUDIO SAMPLE STALLED no samples for %s",
					sampleStallDuration,
				)

				// ------------------------------------------
				// Watchdog เปลี่ยน:
				//
				// HEALTHY
				//    ↓
				// STALLED
				//
				// currentHealth() ตอนนี้จะให้:
				//
				// SampleStalled=true
				//
				// จึงต้องบอก IncidentManager
				// ให้ reconcile ทันที
				// ------------------------------------------

				reconcileIncidents(
					watchdogAt,
				)
			}
		// ==============================================
		// Low Level Window Tick
		// ==============================================

		case tickAt := <-windowTicker.C:
			window := windowAggregator.Flush(
				tickAt,
			)

			// ------------------------------------------
			// Low Level Detector
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
			// LowLevel state อาจเปลี่ยน
			// ------------------------------------------

			reconcileIncidents(
				window.EndAt,
			)

			// ------------------------------------------
			// Terminal Status
			//
			// เพิ่ม sample_state เพื่อดู Watchdog
			// ระหว่างทดลอง
			// ------------------------------------------

			if window.SampleCount == 0 {
				printLog(
					"%s no audio samples sample_state=%s signal_state=%s level_state=%s mute_state=%s routing_state=%s stream_state=%s",
					cfg.OBS.AudioInput,
					sampleWatchdog.State(),
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
					"%s no usable level signal=%t muted=%t sample_state=%s signal_state=%s level_state=%s mute_state=%s routing_state=%s stream_state=%s",
					cfg.OBS.AudioInput,
					latestSample.SignalPresent,
					latestSample.Muted,
					sampleWatchdog.State(),
					signalLossDetector.State(),
					lowLevelDetector.State(),
					muteDetector.State(),
					trackRoutingDetector.State(),
					monitoringContext.StreamingState(),
				)

				continue
			}

			printLog(
				"%s level=%.1f dB signal=%t muted=%t sample_state=%s signal_state=%s level_state=%s mute_state=%s routing_state=%s stream_state=%s",
				cfg.OBS.AudioInput,
				window.MaxDB,
				latestSample.SignalPresent,
				latestSample.Muted,
				sampleWatchdog.State(),
				signalLossDetector.State(),
				lowLevelDetector.State(),
				muteDetector.State(),
				trackRoutingDetector.State(),
				monitoringContext.StreamingState(),
			)

		// ==============================================
		// Heartbeat
		// ==============================================

		case heartbeatAt := <-heartbeatTicker.C:
			heartbeat := buildHeartbeat(
				heartbeatAt,
			)

			if err := agentReporter.SendHeartbeat(
				ctx,
				heartbeat,
			); err != nil {
				printLog(
					"REPORT HEARTBEAT FAILED: %v",
					err,
				)
			}

		// ==============================================
		// Async Reporter Errors
		// ==============================================

		case err := <-reporterErrors:
			printLog(
				"REPORTER ERROR: %v",
				err,
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

func buildIncidentEventID(
	machineID string,
	event incident.Event,
) string {
	raw := fmt.Sprintf(
		"%s|%s|%s|%d",
		machineID,
		event.EventType,
		event.IncidentType,
		event.OccurredAt.UnixNano(),
	)

	sum := sha256.Sum256(
		[]byte(raw),
	)

	// 16 bytes / 128 bits เพียงพอสำหรับ ID ของเรา
	return hex.EncodeToString(
		sum[:16],
	)
}
