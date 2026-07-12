package plugins

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/premchandkpc/FlowRulZ/server/bridge"
)

const maxWasmSize = 10 * 1024 * 1024 // 10MB

var wasmMagic = []byte{0x00, 'a', 's', 'm'}

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
		if len(data) > maxWasmSize {
			return fmt.Errorf("plugin %s: exceeds max size %d bytes", name, maxWasmSize)
		}
		if len(data) >= 4 && !bytes.HasPrefix(data, wasmMagic) {
			slog.Warn("plugins: invalid WASM magic bytes, skipping", "name", name, "path", path)
			continue
		}
		if err := bridge.RegisterPlugin(name, data); err != nil {
			return fmt.Errorf("register plugin %s: %w", name, err)
		}
		slog.Info("plugins: registered plugin", "name", name, "bytes", len(data))
	}

	return nil
}
