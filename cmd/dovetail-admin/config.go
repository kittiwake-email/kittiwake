package main

import (
	"errors"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// settings is the resolved connection configuration.
type settings struct {
	URL   string `yaml:"url"`
	Token string `yaml:"token"`
}

// configPath is ~/.config/dovetail/admin.yaml (or $DOVETAIL_ADMIN_CONFIG).
func configPath() string {
	if p := os.Getenv("DOVETAIL_ADMIN_CONFIG"); p != "" {
		return p
	}
	dir, err := os.UserConfigDir()
	if err != nil {
		return ""
	}
	return filepath.Join(dir, "dovetail", "admin.yaml")
}

// resolve merges flag > env > file. Empty strings mean "not set".
func resolve(flagURL, flagToken string, env func(string) string, file string) (settings, error) {
	var s settings
	if file != "" {
		b, err := os.ReadFile(file)
		if err == nil {
			if err := yaml.Unmarshal(b, &s); err != nil {
				return settings{}, errors.New("config " + file + ": " + err.Error())
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return settings{}, err
		}
	}
	if v := env("DOVETAIL_ADMIN_URL"); v != "" {
		s.URL = v
	}
	if v := env("DOVETAIL_ADMIN_TOKEN"); v != "" {
		s.Token = v
	}
	if flagURL != "" {
		s.URL = flagURL
	}
	if flagToken != "" {
		s.Token = flagToken
	}
	if s.URL == "" {
		s.URL = "http://[::1]:8081"
	}
	return s, nil
}
