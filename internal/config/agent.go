package config

import (
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type AgentConfig struct {
	Agent   AgentInfoConfig `yaml:"agent"`
	Backend BackendConfig   `yaml:"backend"`
	OBS     OBSConfig       `yaml:"obs"`
	Audio   AudioConfig     `yaml:"audio"`
}

type OBSConfig struct {
	Host       string `yaml:"host"`
	Port       int    `yaml:"port"`
	Password   string `yaml:"password"`
	AudioInput string `yaml:"audio_input"`
}

type AudioConfig struct {
	SignalLossDuration string `yaml:"signal_loss_duration"`

	LowLevelThresholdDB    float64 `yaml:"low_level_threshold_db"`
	LowLevelDuration       string  `yaml:"low_level_duration"`
	LowLevelWindowDuration string  `yaml:"low_level_window_duration"`

	RecoveryDuration string `yaml:"recovery_duration"`
}

type AgentInfoConfig struct {
	MachineID         string `yaml:"machine_id"`
	HeartbeatInterval string `yaml:"heartbeat_interval"`
}

type BackendConfig struct {
	Enabled        bool   `yaml:"enabled"`
	BaseURL        string `yaml:"base_url"`
	AgentToken     string `yaml:"agent_token"`
	RequestTimeout string `yaml:"request_timeout"`
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

	// --------------------------------------------------
	// Defaults
	// --------------------------------------------------

	if cfg.OBS.Host == "" {
		cfg.OBS.Host = "127.0.0.1"
	}

	if cfg.OBS.Port == 0 {
		cfg.OBS.Port = 4455
	}

	if cfg.Audio.SignalLossDuration == "" {
		cfg.Audio.SignalLossDuration = "10s"
	}

	if cfg.Audio.LowLevelThresholdDB == 0 {
		cfg.Audio.LowLevelThresholdDB = -60
	}

	if cfg.Audio.LowLevelDuration == "" {
		cfg.Audio.LowLevelDuration = "10s"
	}

	if cfg.Audio.LowLevelWindowDuration == "" {
		cfg.Audio.LowLevelWindowDuration = "1s"
	}

	if cfg.Audio.RecoveryDuration == "" {
		cfg.Audio.RecoveryDuration = "5s"
	}

	if cfg.Agent.HeartbeatInterval == "" {
		cfg.Agent.HeartbeatInterval = "5s"
	}

	if cfg.Backend.RequestTimeout == "" {
		cfg.Backend.RequestTimeout = "2s"
	}

	// --------------------------------------------------
	// Validation
	// --------------------------------------------------

	if cfg.Agent.MachineID == "" {
		return nil, errors.New(
			"agent.machine_id is required",
		)
	}

	if _, err := time.ParseDuration(
		cfg.Agent.HeartbeatInterval,
	); err != nil {
		return nil, fmt.Errorf(
			"invalid agent.heartbeat_interval: %w",
			err,
		)
	}

	if cfg.Backend.Enabled {
		if cfg.Backend.BaseURL == "" {
			return nil, errors.New(
				"backend.base_url is required",
			)
		}

		if cfg.Backend.AgentToken == "" {
			return nil, errors.New(
				"backend.agent_token is required",
			)
		}

		if _, err := time.ParseDuration(
			cfg.Backend.RequestTimeout,
		); err != nil {
			return nil, fmt.Errorf(
				"invalid backend.request_timeout: %w",
				err,
			)
		}
	}

	if cfg.OBS.AudioInput == "" {
		return nil, fmt.Errorf(
			"obs.audio_input is required",
		)
	}

	if cfg.Audio.LowLevelThresholdDB >= 0 {
		return nil, fmt.Errorf(
			"audio.low_level_threshold_db must be below 0 dB",
		)
	}

	if err := validateDuration(
		"audio.signal_loss_duration",
		cfg.Audio.SignalLossDuration,
	); err != nil {
		return nil, err
	}

	if err := validateDuration(
		"audio.low_level_duration",
		cfg.Audio.LowLevelDuration,
	); err != nil {
		return nil, err
	}

	if err := validateDuration(
		"audio.low_level_window_duration",
		cfg.Audio.LowLevelWindowDuration,
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
