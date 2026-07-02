package plugins

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/premchandkpc/FlowRulZ/server/bridge"
)

func LoadDir(pluginDir string) error {
	entries, err := os.ReadDir(pluginDir)
	if err != nil {
		if os.IsNotExist(err) {
			slog.Info("plugins: dir does not exist, skipping", "dir", pluginDir)
			return nil
		}
		return fmt.Errorf("read plugin dir %s: %w", pluginDir, err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if filepath.Ext(entry.Name()) != ".wasm" {
			continue
		}
		name := entry.Name()[:len(entry.Name())-5]
		path := filepath.Join(pluginDir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read plugin %s: %w", path, err)
		}
		if err := bridge.RegisterPlugin(name, data); err != nil {
			return fmt.Errorf("register plugin %s: %w", name, err)
		}
		slog.Info("plugins: registered plugin", "name", name, "bytes", len(data))
	}

	return nil
}
