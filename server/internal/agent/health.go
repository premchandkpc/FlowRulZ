package agent

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// HealthStatus represents the health of an agent or pool.
type HealthStatus int

const (
	HealthHealthy  HealthStatus = iota
	HealthDegraded
	HealthUnhealthy
)

func (h HealthStatus) String() string {
	switch h {
	case HealthHealthy:
		return "healthy"
	case HealthDegraded:
		return "degraded"
	case HealthUnhealthy:
		return "unhealthy"
	default:
		return "unknown"
	}
}

// HealthCheck defines a health check function.
type HealthCheck struct {
	Name     string
	Check    func(ctx context.Context) error
	Timeout  time.Duration
	Critical bool // If true, failure makes the system unhealthy
}

// HealthMonitor monitors the health of agents and the pool.
type HealthMonitor struct {
	mu       sync.RWMutex
	checks   []HealthCheck
	status   HealthStatus
	lastRun  time.Time
	lastErr  error
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// NewHealthMonitor creates a new health monitor.
func NewHealthMonitor() *HealthMonitor {
	return &HealthMonitor{
		status: HealthHealthy,
		stopCh: make(chan struct{}),
	}
}

// Register adds a health check.
func (hm *HealthMonitor) Register(check HealthCheck) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	if check.Timeout <= 0 {
		check.Timeout = 5 * time.Second
	}
	hm.checks = append(hm.checks, check)
}

// Start begins periodic health checks.
func (hm *HealthMonitor) Start(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 10 * time.Second
	}

	hm.wg.Add(1)
	go func() {
		defer hm.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		// Run initial check
		hm.runChecks(ctx)

		for {
			select {
			case <-hm.stopCh:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				hm.runChecks(ctx)
			}
		}
	}()
}

// Stop stops the health monitor.
func (hm *HealthMonitor) Stop() {
	hm.stopOnce.Do(func() {
		close(hm.stopCh)
	})
	hm.wg.Wait()
}

// Status returns the current health status.
func (hm *HealthMonitor) Status() HealthStatus {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	return hm.status
}

// CheckNow runs all health checks immediately and returns the status.
func (hm *HealthMonitor) CheckNow(ctx context.Context) (HealthStatus, error) {
	return hm.runChecks(ctx)
}

func (hm *HealthMonitor) runChecks(ctx context.Context) (HealthStatus, error) {
	hm.mu.Lock()
	checks := make([]HealthCheck, len(hm.checks))
	copy(checks, hm.checks)
	hm.mu.Unlock()

	status := HealthHealthy
	var lastErr error

	for _, check := range checks {
		checkCtx, cancel := context.WithTimeout(ctx, check.Timeout)
		err := check.Check(checkCtx)
		cancel()

		if err != nil {
			lastErr = fmt.Errorf("%s: %w", check.Name, err)
			if check.Critical {
				status = HealthUnhealthy
			} else if status == HealthHealthy {
				status = HealthDegraded
			}
			slog.Warn("health check failed",
				"check", check.Name,
				"error", err,
				"critical", check.Critical)
		}
	}

	hm.mu.Lock()
	hm.status = status
	hm.lastRun = time.Now()
	hm.lastErr = lastErr
	hm.mu.Unlock()

	return status, lastErr
}

// HealthReport is a snapshot of health status.
type HealthReport struct {
	Status    HealthStatus `json:"status"`
	LastRun   time.Time    `json:"last_run"`
	LastError string       `json:"last_error,omitempty"`
	Checks    int          `json:"checks"`
}

// Report returns the current health report.
func (hm *HealthMonitor) Report() HealthReport {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	var errStr string
	if hm.lastErr != nil {
		errStr = hm.lastErr.Error()
	}
	return HealthReport{
		Status:    hm.status,
		LastRun:   hm.lastRun,
		LastError: errStr,
		Checks:    len(hm.checks),
	}
}
