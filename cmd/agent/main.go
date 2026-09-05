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
	obsclient "live-monitor/internal/agent/obs"
	"live-monitor/internal/config"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	configPath := flag.String(
		"config",
		"configs/agent.yaml",
		"path to agent config file",
	)

	flag.Parse()

	cfg, err := config.LoadAgent(*configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	printInterval, err := time.ParseDuration(
		cfg.Probe.PrintInterval,
	)
	if err != nil {
		return fmt.Errorf(
			"parse print interval: %w",
			err,
		)
	}

	client, err := obsclient.New(
		cfg.OBS.Host,
		cfg.OBS.Port,
		cfg.OBS.Password,
	)
	if err != nil {
		return fmt.Errorf("connect OBS: %w", err)
	}
	defer client.Close()

	printLog("Connected to OBS")

	if err := client.EnsureInput(
		cfg.OBS.AudioInput,
	); err != nil {
		return err
	}

	printLog(
		"Input found: %s",
		cfg.OBS.AudioInput,
	)

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
	)
	defer stop()

	samples := make(chan audio.Sample, 64)
	streamErr := make(chan error, 1)

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

	ticker := time.NewTicker(printInterval)
	defer ticker.Stop()

	var latestSample audio.Sample
	var hasSample bool

	for {
		select {

		case <-ctx.Done():
			printLog("Agent stopped")
			return nil

		case err := <-streamErr:
			if err != nil {
				return fmt.Errorf(
					"audio sample stream stopped: %w",
					err,
				)
			}

			printLog("Agent stopped")
			return nil

		case sample := <-samples:
			latestSample = sample
			hasSample = true

		case <-ticker.C:
			if !hasSample {
				continue
			}

			printLog(
				"%s level=%.1f dB muted=%t",
				latestSample.InputName,
				latestSample.LevelDB,
				latestSample.Muted,
			)
		}
	}
}

func printLog(format string, args ...any) {
	message := fmt.Sprintf(format, args...)

	fmt.Printf(
		"[%s] %s\n",
		time.Now().Format("15:04:05"),
		message,
	)
}
