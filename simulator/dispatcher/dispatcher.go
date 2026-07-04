package dispatcher

import (
	"hash/fnv"
	"log/slog"

	"github.com/premchandkpc/FlowRulZ/simulator/execution"
	"github.com/premchandkpc/FlowRulZ/simulator/scheduler"
	"github.com/premchandkpc/FlowRulZ/simulator/timeline"
)

type Dispatcher struct {
	Nodes     []*scheduler.Scheduler
	Timeline  *timeline.Store
}

func New(nodes []*scheduler.Scheduler, tl *timeline.Store) *Dispatcher {
	return &Dispatcher{
		Nodes:    nodes,
		Timeline: tl,
	}
}

func (d *Dispatcher) Dispatch(ctx *execution.ExecutionContext) {
	if len(d.Nodes) == 0 {
		slog.Warn("dispatcher: no nodes available", "exec_id", ctx.ID)
		return
	}
	idx := d.hashNode(ctx.ID, len(d.Nodes))
	node := d.Nodes[idx]

	d.Timeline.Record(timeline.Event{
		ExecID:    ctx.ID,
		Timestamp: ctx.CreatedAt,
		Type:      timeline.EventCreated,
		Meta:      ctx.Plan.ID,
		NodeID:    node.ID,
	})

	node.Enqueue(ctx)
}

func (d *Dispatcher) hashNode(key string, n int) int {
	if n == 0 {
		return 0
	}
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % n
}

func (d *Dispatcher) StartAll() {
	slog.Info("dispatcher: starting", "nodes", len(d.Nodes))
	for _, node := range d.Nodes {
		node.Start()
	}
}

func (d *Dispatcher) StopAll() {
	for _, node := range d.Nodes {
		node.Stop()
	}
}
