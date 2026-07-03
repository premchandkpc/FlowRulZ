package bootstrap

import (
	"context"
	"fmt"

	"github.com/premchandkpc/FlowRulZ/server/internal/common"
	"github.com/premchandkpc/FlowRulZ/server/internal/engine"
	"github.com/premchandkpc/FlowRulZ/server/internal/node"
	"github.com/premchandkpc/FlowRulZ/server/internal/scheduler"
)

type NodeBuilder struct {
	cfg  node.Config
	deps node.Dependencies
	errs []error

	lifecycle *common.LifecycleRegistry
}

func NewNodeBuilder(cfg node.Config) *NodeBuilder {
	return &NodeBuilder{
		cfg:       cfg,
		lifecycle: common.NewLifecycleRegistry(),
	}
}

func (b *NodeBuilder) WithDefaults() *NodeBuilder {
	b.deps = node.DefaultDependencies(b.cfg)
	return b
}

func (b *NodeBuilder) Build() (*node.ProdNode, error) {
	if len(b.errs) > 0 {
		return nil, fmt.Errorf("bootstrap: %d errors: %v", len(b.errs), b.errs[0])
	}
	pn := node.NewNode(b.cfg, b.deps)
	if len(b.errs) > 0 {
		return nil, fmt.Errorf("bootstrap: %d errors: %v", len(b.errs), b.errs[0])
	}
	return pn, nil
}

func (b *NodeBuilder) BuildWithLifecycle(ctx context.Context) (*node.ProdNode, error) {
	pn, err := b.Build()
	if err != nil {
		return nil, err
	}
	return pn, nil
}

func (b *NodeBuilder) Lifecycle() *common.LifecycleRegistry {
	return b.lifecycle
}

func (b *NodeBuilder) register(name string, svc common.Service) {
	b.lifecycle.Register(name, svc)
}

// --- adapters for lifecycle ---

type engineService struct{ e *engine.Engine }

func (s engineService) Start(ctx context.Context) error { return nil }
func (s engineService) Stop() error                     { return nil }

type schedulerService struct{ s *scheduler.Scheduler }

func (s schedulerService) Start(ctx context.Context) error { return s.s.Start(ctx) }
func (s schedulerService) Stop() error                     { return s.s.Stop() }
