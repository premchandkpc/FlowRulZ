package node

import (
	"net/http"

	"github.com/premchandkpc/FlowRulZ/server/internal/execstate"
	"github.com/premchandkpc/FlowRulZ/server/internal/ports"
	pkgcluster "github.com/premchandkpc/FlowRulZ/server/pkg/cluster"
	pkgmembership "github.com/premchandkpc/FlowRulZ/server/pkg/membership"
	pkgpartition "github.com/premchandkpc/FlowRulZ/server/pkg/partition"
	pkgreplyrouter "github.com/premchandkpc/FlowRulZ/server/pkg/replyrouter"
	pkgscheduler "github.com/premchandkpc/FlowRulZ/server/pkg/scheduler"
)

type NodeConfig struct {
	Config
	httpClient *http.Client
}

type ClusterDeps struct {
	RaftCluster pkgcluster.ClusterMember
	ClusterNode ClusterTransport
	Membership  pkgmembership.Membership
}

type TransportDeps struct {
	TransportFactory TransportFactory
	GRPCBus          GRPCService
}

type ExecutionDeps struct {
	Engine     NodeEngine
	Scheduler  pkgscheduler.Scheduler
	StateStore StateStore
	Execs      execstate.ExecRegistry
	Saga       NodeSagaTracker
	Invoker    ServiceInvoker
}

type ReliabilityDeps struct {
	DLQ         NodeDLQ
	RateLimiter RateLimiter
	Dedup       DedupChecker
}

type APIDeps struct {
	AdminSrv     AdminHandler
	Registry     ServiceLookup
	ReplyRouter  pkgreplyrouter.ReplyRouter
	Metrics      ports.MetricsCollector
	OtelExporter SpanExporter
}

type PartitionDeps struct {
	Partitions pkgpartition.PartitionManager
	Rebalancer pkgpartition.RebalanceNotifier
	PlanDist   PlanDistributor
}
