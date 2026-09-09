package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type AgentConfig struct {
	MachineID string      `yaml:"machine_id"`
	OBS       OBSConfig   `yaml:"obs"`
	Probe     ProbeConfig `yaml:"probe"`
	Audio     AudioConfig `yaml:"audio"`
}

type OBSConfig struct {
	Host       string `yaml:"host"`
	Port       int    `yaml:"port"`
	Password   string `yaml:"password"`
	AudioInput string `yaml:"audio_input"`
}

type ProbeConfig struct {
	PrintInterval string `yaml:"print_interval"`
}

type AudioConfig struct {
	SignalLossDuration string `yaml:"signal_loss_duration"`
	RecoveryDuration   string `yaml:"recovery_duration"`
}

func LoadAgent(path string) (*AgentConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf(
			"read agent config: %w",
			err,
		)
	}

	var cfg AgentConfig

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf(
			"parse agent config: %w",
			err,
		)
	}

	if cfg.OBS.Host == "" {
		cfg.OBS.Host = "127.0.0.1"
	}

	if cfg.OBS.Port == 0 {
		cfg.OBS.Port = 4455
	}

	if cfg.Probe.PrintInterval == "" {
		cfg.Probe.PrintInterval = "1s"
	}

	if cfg.Audio.SignalLossDuration == "" {
		cfg.Audio.SignalLossDuration = "10s"
	}

	if cfg.Audio.RecoveryDuration == "" {
		cfg.Audio.RecoveryDuration = "5s"
	}

	if cfg.MachineID == "" {
		return nil, fmt.Errorf(
			"machine_id is required",
		)
	}

	if cfg.OBS.AudioInput == "" {
		return nil, fmt.Errorf(
			"obs.audio_input is required",
		)
	}

	if err := validateDuration(
		"probe.print_interval",
		cfg.Probe.PrintInterval,
	); err != nil {
		return nil, err
	}

	if err := validateDuration(
		"audio.signal_loss_duration",
		cfg.Audio.SignalLossDuration,
	); err != nil {
		return nil, err
	}

	if err := validateDuration(
		"audio.recovery_duration",
		cfg.Audio.RecoveryDuration,
	); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func validateDuration(
	name string,
	value string,
) error {
	duration, err := time.ParseDuration(value)
	if err != nil {
		return fmt.Errorf(
			"invalid %s %q: %w",
			name,
			value,
			err,
		)
	}

	if duration <= 0 {
		return fmt.Errorf(
			"%s must be greater than 0",
			name,
		)
	}

	return nil
}
