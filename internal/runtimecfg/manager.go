package runtimecfg

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/pcdogyu/A-Stock-Order-Flow/internal/config"
	"gopkg.in/yaml.v3"
)

type Manager struct {
	path string
	mu   sync.RWMutex
	cfg  config.Config
}

func Load(path string) (*Manager, error) {
	cfg, err := config.Load(path)
	if err != nil {
		return nil, err
	}
	return &Manager{path: path, cfg: cfg}, nil
}

func NewStatic(cfg config.Config) *Manager {
	return &Manager{cfg: cfg}
}

func (m *Manager) Get() config.Config {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.cfg
}

func (m *Manager) Update(p Patch) (config.Config, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cfg := m.cfg
	p.Apply(&cfg)
	if err := config.NormalizeAndValidate(&cfg); err != nil {
		return config.Config{}, err
	}
	m.cfg = cfg

	if m.path != "" {
		if err := m.saveLocked(); err != nil {
			return config.Config{}, err
		}
	}
	return cfg, nil
}

func (m *Manager) saveLocked() error {
	b, err := yaml.Marshal(&m.cfg)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(m.path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(m.path, b, 0o644)
}

