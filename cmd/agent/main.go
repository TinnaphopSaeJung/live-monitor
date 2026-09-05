package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	obsclient "live-monitor/internal/agent/obs"
	"live-monitor/internal/config"
)

func main() {
	configPath := flag.String(
		"config",
		"configs/agent.yaml",
		"path to agent config file",
	)

	flag.Parse()

	cfg, err := config.LoadAgent(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	printInterval, err := time.ParseDuration(
		cfg.Probe.PrintInterval,
	)
	if err != nil {
		log.Fatalf("parse print interval: %v", err)
	}

	client, err := obsclient.New(
		cfg.OBS.Host,
		cfg.OBS.Port,
		cfg.OBS.Password,
	)
	if err != nil {
		log.Fatalf("connect OBS: %v", err)
	}
	defer client.Close()

	printLog("Connected to OBS")

	if err := client.EnsureInput(cfg.OBS.AudioInput); err != nil {
		log.Fatal(err)
	}

	printLog(
		"Input found: %s",
		cfg.OBS.AudioInput,
	)

	printLog(
		"Listening to audio meter... (Ctrl+C to stop)",
	)

	ctx, stop := signal.NotifyContext( // สร้าง Context ที่จะถูก cancel เมื่อ program ได้รับ interrupt
		context.Background(),
		os.Interrupt,
	)
	defer stop()

	err = client.RunAudioProbe(
		ctx,
		cfg.OBS.AudioInput,
		printInterval,
		func(sample obsclient.AudioSample) {
			printLog(
				"%s level=%.1f dB muted=%t",
				sample.InputName,
				sample.LevelDB,
				sample.Muted,
			)
		},
	)

	if err != nil {
		log.Fatalf("audio probe stopped: %v", err)
	}

	printLog("Agent stopped")
}

func printLog(format string, args ...any) {
	message := fmt.Sprintf(format, args...)

	fmt.Printf(
		"[%s] %s\n",
		time.Now().Format("15:04:05"),
		message,
	)
}
