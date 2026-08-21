package agent

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/premchandkpc/FlowRulZ/server/internal/observability"
)

// PoolConfig holds agent pool configuration.
type PoolConfig struct {
	MinAgents          int
	MaxAgents          int
	QueueSize          int
	ExecTimeout        time.Duration
	HealthCheck        time.Duration
	ScaleUpThreshold   float64       // queue depth ratio to trigger scale up
	ScaleDownThreshold time.Duration // idle time to trigger scale down
}

// DefaultPoolConfig returns sensible defaults.
func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		MinAgents:          2,
		MaxAgents:          16,
		QueueSize:          10000,
		ExecTimeout:        30 * time.Second,
		HealthCheck:        5 * time.Second,
		ScaleUpThreshold:   0.8,
		ScaleDownThreshold: 30 * time.Second,
	}
}

// AgentPool manages a pool of agents with dynamic scaling.
type AgentPool struct {
	config   PoolConfig
	queue    *TaskQueue
	agents   map[string]*Agent
	mu       sync.RWMutex
	nextID   atomic.Int64
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
	state    atomic.Int32
	ctx      context.Context

	// Metrics
	totalSubmitted atomic.Int64
	totalCompleted atomic.Int64
	totalFailed    atomic.Int64

	// Scaling
	lastScaleTime time.Time

	// Observability
	metrics      *observability.MetricsCollector
	submitCount  *observability.Counter
	completeCount *observability.Counter
	failCount    *observability.Counter
	agentGauge   *observability.Gauge
	queueGauge   *observability.Gauge
	scaleUpCount *observability.Counter
	scaleDownCount *observability.Counter
}

// NewPool creates a new agent pool.
func NewPool(config PoolConfig) *AgentPool {
	if config.MinAgents <= 0 {
		config.MinAgents = 2
	}
	if config.MaxAgents <= 0 {
		config.MaxAgents = 16
	}
	if config.MaxAgents < config.MinAgents {
		config.MaxAgents = config.MinAgents
	}
	if config.QueueSize <= 0 {
		config.QueueSize = 10000
	}
	if config.ExecTimeout <= 0 {
		config.ExecTimeout = 30 * time.Second
	}
	if config.HealthCheck <= 0 {
		config.HealthCheck = 5 * time.Second
	}
	if config.ScaleUpThreshold <= 0 {
		config.ScaleUpThreshold = 0.8
	}
	if config.ScaleDownThreshold <= 0 {
		config.ScaleDownThreshold = 30 * time.Second
	}

	mc := observability.NewMetricsCollector()

	return &AgentPool{
		config:        config,
		queue:         NewTaskQueue(config.QueueSize),
		agents:        make(map[string]*Agent),
		stopCh:        make(chan struct{}),
		lastScaleTime: time.Now(),
		metrics:       mc,
		submitCount:   mc.Counter("agent.submit_total"),
		completeCount: mc.Counter("agent.complete_total"),
		failCount:     mc.Counter("agent.fail_total"),
		agentGauge:    mc.Gauge("agent.active_count"),
		queueGauge:    mc.Gauge("agent.queue_depth"),
		scaleUpCount:  mc.Counter("agent.scale_up_total"),
		scaleDownCount: mc.Counter("agent.scale_down_total"),
	}
}

// Start initializes the pool with MinAgents workers and starts health monitoring.
func (p *AgentPool) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.ctx = ctx

	// Start minimum agents
	for i := 0; i < p.config.MinAgents; i++ {
		if err := p.spawnAgentLocked(); err != nil {
			return fmt.Errorf("pool: spawn initial agent %d: %w", i, err)
		}
	}

	// Start health checker
	p.wg.Add(1)
	go p.healthLoop()

	slog.Info("agent pool: started",
		"min_agents", p.config.MinAgents,
		"max_agents", p.config.MaxAgents,
		"queue_size", p.config.QueueSize)
	return nil
}

// Stop signals all agents to stop and waits for them to finish.
func (p *AgentPool) Stop() {
	p.stopOnce.Do(func() {
		p.state.Store(int32(StateStopped))
		close(p.stopCh)
		p.queue.Stop()
	})

	p.mu.RLock()
	agents := make([]*Agent, 0, len(p.agents))
	for _, a := range p.agents {
		agents = append(agents, a)
	}
	p.mu.RUnlock()

	for _, a := range agents {
		a.StopAsync()
	}
	for _, a := range agents {
		a.Wait()
	}

	p.wg.Wait()
	slog.Info("agent pool: stopped",
		"tasks_submitted", p.totalSubmitted.Load(),
		"tasks_completed", p.totalCompleted.Load(),
		"tasks_failed", p.totalFailed.Load())
}

// Submit adds a task to the pool. Returns false if the pool is stopped or queue is full.
func (p *AgentPool) Submit(task *Task) bool {
	if task == nil {
		return false
	}
	p.mu.RLock()
	stopped := p.state.Load() == int32(StateStopped)
	p.mu.RUnlock()
	if stopped {
		return false
	}
	p.totalSubmitted.Add(1)
	p.submitCount.Inc()
	return p.queue.Enqueue(task)
}

// SubmitWait adds a task, waiting until space is available.
func (p *AgentPool) SubmitWait(ctx context.Context, task *Task) bool {
	if task == nil {
		return false
	}
	p.totalSubmitted.Add(1)
	p.submitCount.Inc()
	return p.queue.EnqueueWait(ctx, task)
}

// SubmitAndWait submits a task and waits for the result.
func (p *AgentPool) SubmitAndWait(ctx context.Context, task *Task) ([]byte, error) {
	if task == nil {
		return nil, fmt.Errorf("nil task")
	}

	if task.ResultCh == nil {
		task.ResultCh = make(chan TaskResult, 1)
	}

	p.totalSubmitted.Add(1)
	p.submitCount.Inc()
	if !p.queue.EnqueueWait(ctx, task) {
		return nil, fmt.Errorf("pool: queue full or stopped")
	}

	select {
	case result := <-task.ResultCh:
		if result.Error != nil {
			p.totalFailed.Add(1)
			p.failCount.Inc()
			return nil, result.Error
		}
		p.totalCompleted.Add(1)
		p.completeCount.Inc()
		return result.Output, nil
	case <-ctx.Done():
		p.totalFailed.Add(1)
		p.failCount.Inc()
		return nil, ctx.Err()
	case <-p.stopCh:
		p.totalFailed.Add(1)
		p.failCount.Inc()
		return nil, fmt.Errorf("pool: stopped")
	}
}

// Stats returns pool statistics.
func (p *AgentPool) Stats() PoolStats {
	p.mu.RLock()
	agentCount := len(p.agents)
	p.mu.RUnlock()

	queueStats := p.queue.Stats()

	return PoolStats{
		AgentCount:   agentCount,
		MinAgents:    p.config.MinAgents,
		MaxAgents:    p.config.MaxAgents,
		Queue:        queueStats,
		Submitted:    p.totalSubmitted.Load(),
		Completed:    p.totalCompleted.Load(),
		Failed:       p.totalFailed.Load(),
	}
}

// AgentStats returns stats for all agents.
func (p *AgentPool) AgentStats() []AgentStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	stats := make([]AgentStats, 0, len(p.agents))
	for _, a := range p.agents {
		stats = append(stats, a.Stats())
	}
	return stats
}

// Queue returns the underlying task queue (for testing).
func (p *AgentPool) Queue() *TaskQueue {
	return p.queue
}

func (p *AgentPool) spawnAgentLocked() error {
	id := fmt.Sprintf("agent-%d", p.nextID.Add(1))
	a := NewAgent(id, p.queue,
		WithExecTimeout(p.config.ExecTimeout),
		WithPanicHandler(func(agentID string, r any) {
			slog.Error("agent: panic in pool", "agent_id", agentID, "panic", r)
		}),
	)
	p.agents[id] = a

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		a.Start(p.ctx)
	}()
	return nil
}

func (p *AgentPool) healthLoop() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.config.HealthCheck)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			return
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.checkHealth()
		}
	}
}

func (p *AgentPool) checkHealth() {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	queueLen := int64(p.queue.Len())
	queueCap := int64(p.queue.Cap())
	loadRatio := float64(queueLen) / float64(queueCap)

	// Update metrics
	p.agentGauge.Set(int64(len(p.agents)))
	p.queueGauge.Set(queueLen)

	// Scale up if queue is congested and we haven't scaled recently
	if loadRatio > p.config.ScaleUpThreshold && len(p.agents) < p.config.MaxAgents {
		if now.Sub(p.lastScaleTime) > 5*time.Second {
			if err := p.spawnAgentLocked(); err != nil {
				slog.Error("pool: scale up failed", "error", err)
			} else {
				p.lastScaleTime = now
				p.scaleUpCount.Inc()
				slog.Info("pool: scaled up",
					"agents", len(p.agents),
					"queue_depth", queueLen,
					"load_ratio", fmt.Sprintf("%.1f%%", loadRatio*100))
			}
		}
	}

	// Scale down if agents are idle and we have more than minimum
	if len(p.agents) > p.config.MinAgents {
		if now.Sub(p.lastScaleTime) > p.config.ScaleDownThreshold {
			// Find the oldest idle agent
			var oldest *Agent
			var oldestTime time.Time
			for _, a := range p.agents {
				if a.State() == StateIdle {
					lastTask, _ := a.lastTaskTime.Load().(time.Time)
					if oldest == nil || lastTask.Before(oldestTime) {
						oldest = a
						oldestTime = lastTask
					}
				}
			}
			if oldest != nil && now.Sub(oldestTime) > p.config.ScaleDownThreshold {
				oldest.StopAsync()
				delete(p.agents, oldest.ID())
				p.lastScaleTime = now
				p.scaleDownCount.Inc()
				slog.Info("pool: scaled down",
					"agents", len(p.agents),
					"removed", oldest.ID())
			}
		}
	}
}

// PoolStats holds pool statistics.
type PoolStats struct {
	AgentCount int        `json:"agent_count"`
	MinAgents  int        `json:"min_agents"`
	MaxAgents  int        `json:"max_agents"`
	Queue      QueueStats `json:"queue"`
	Submitted  int64      `json:"submitted"`
	Completed  int64      `json:"completed"`
	Failed     int64      `json:"failed"`
}
