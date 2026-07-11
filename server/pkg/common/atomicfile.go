package common

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// WriteJSON atomically writes a JSON-encoded value to path.
// Uses marshal → tmp → rename to prevent partial writes on crash.
func WriteJSON(path string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// ReadJSON reads and decodes a JSON file into v.
func ReadJSON(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

// LoadDir scans dir for files with the given extension, reads each, and
// decodes into T using the provided unmarshal function.
func LoadDir[T any](dir, ext string, decode func([]byte) (T, error)) ([]T, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []T
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ext {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			continue
		}
		v, err := decode(data)
		if err != nil {
			continue
		}
		out = append(out, v)
	}
	return out, nil
}
