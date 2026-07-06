package flow

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// FileWatcher watches .flow files for changes.
type FileWatcher struct {
	registry *Registry
	dirs     []string
	interval time.Duration
	mu       sync.Mutex
	running  bool
	stop     chan struct{}
}

// NewFileWatcher creates a new file watcher.
func NewFileWatcher(registry *Registry, dirs ...string) *FileWatcher {
	return &FileWatcher{
		registry: registry,
		dirs:     dirs,
		interval: 5 * time.Second,
		stop:     make(chan struct{}),
	}
}

// SetInterval sets the watch interval.
func (w *FileWatcher) SetInterval(d time.Duration) {
	w.interval = d
}

// Start begins watching for changes.
func (w *FileWatcher) Start(ctx context.Context) error {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return nil
	}
	w.running = true
	w.mu.Unlock()

	go w.watch(ctx)
	return nil
}

// Stop stops watching.
func (w *FileWatcher) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.running {
		close(w.stop)
		w.running = false
	}
}

func (w *FileWatcher) watch(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	// Initial load
	w.loadAll(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stop:
			return
		case <-ticker.C:
			w.checkForChanges(ctx)
		}
	}
}

func (w *FileWatcher) loadAll(ctx context.Context) {
	for _, dir := range w.dirs {
		if err := w.registry.LoadDirectory(ctx, dir); err != nil {
			slog.Error("flow: load directory", "dir", dir, "error", err)
		}
	}
}

func (w *FileWatcher) checkForChanges(ctx context.Context) {
	for _, dir := range w.dirs {
		w.walkDir(ctx, dir)
	}
}

func (w *FileWatcher) walkDir(ctx context.Context, dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}

	for _, entry := range entries {
		path := filepath.Join(dir, entry.Name())

		if entry.IsDir() {
			w.walkDir(ctx, path)
			continue
		}

		if filepath.Ext(entry.Name()) != ".flow" {
			continue
		}

		// Check modification time
		info, err := entry.Info()
		if err != nil {
			continue
		}

		// Reload if modified recently
		if time.Since(info.ModTime()) < w.interval {
			slog.Info("flow: reloading", "file", path)
			if err := w.registry.LoadFile(ctx, path); err != nil {
				slog.Error("flow: reload failed", "file", path, "error", err)
			}
		}
	}
}

// DebouncedWatcher debounces file change events.
type DebouncedWatcher struct {
	watcher *FileWatcher
	delay   time.Duration
	timer   *time.Timer
	mu      sync.Mutex
}

// NewDebouncedWatcher creates a debounced file watcher.
func NewDebouncedWatcher(watcher *FileWatcher, delay time.Duration) *DebouncedWatcher {
	return &DebouncedWatcher{
		watcher: watcher,
		delay:   delay,
	}
}

// Notify signals a file change (debounced).
func (d *DebouncedWatcher) Notify() {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.timer != nil {
		d.timer.Stop()
	}

	d.timer = time.AfterFunc(d.delay, func() {
		d.watcher.checkForChanges(context.Background())
	})
}
