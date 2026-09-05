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

func LoadAgent(path string) (*AgentConfig, error) {
	data, err := os.ReadFile(path) // อ่านออกมาได้เป็น []byte
	if err != nil {
		return nil, fmt.Errorf("read agent config: %w", err)
	}

	var cfg AgentConfig

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse agent config: %w", err)
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

	if cfg.MachineID == "" {
		return nil, fmt.Errorf("machine_id is required")
	}

	if cfg.OBS.AudioInput == "" {
		return nil, fmt.Errorf("obs.audio_input is required")
	}

	if _, err := time.ParseDuration(cfg.Probe.PrintInterval); err != nil {
		return nil, fmt.Errorf(
			"invalid probe.print_interval %q: %w",
			cfg.Probe.PrintInterval,
			err,
		)
	}

	return &cfg, nil
}
