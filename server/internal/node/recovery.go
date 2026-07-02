package node

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/premchandkpc/FlowRulZ/go/bridge"
	"github.com/premchandkpc/FlowRulZ/go/internal/execstate"
)

func (n *ProdNode) tryCompensate(execID string) {
	if n.Saga == nil {
		return
	}
	if err := n.Saga.Compensate(execID); err != nil {
		slog.Error("saga: compensation error", "exec_id", execID, "error", err)
	}
}

func (n *ProdNode) recoverInFlight(ctx context.Context) {
	if n.StateStore == nil {
		return
	}

	inflight, err := n.StateStore.ListByStatus(ctx, execstate.StatusRunning, execstate.StatusWaitingForService)
	if err != nil {
		slog.Error("recovery: list error", "error", err)
		return
	}

	for _, st := range inflight {
		go n.recoverExecution(st)
	}
}

func (n *ProdNode) recoverExecution(st *execstate.State) {
	slog.Info("recovery: resuming execution", "exec_id", st.ID, "status", st.Status, "rule_id", st.RuleID)

	names := make(map[uint16]string)
	if entries, err := bridge.PlanServices(st.PlanBytes); err == nil {
		for _, e := range entries {
			names[e.ID] = e.Name
		}
	}

	var startResp []byte
	if st.Status == execstate.StatusWaitingForService {
		rawName, ok := names[st.PendingSvc]
		if !ok {
			rawName = fmt.Sprintf("svc-%d", st.PendingSvc)
		}
		svcName, method := bridge.ParseServiceMethod(rawName)
		resp, err := n.callService(svcName, method, st.PendingBody, 0)
		if err != nil {
			slog.Warn("recovery: exec retry failed", "exec_id", st.ID, "service", svcName, "error", err)
			st.Status = execstate.StatusFailed
			st.Error = fmt.Sprintf("recovery retry: %v", err)
			n.StateStore.Save(context.Background(), st)
			return
		}
		startResp = resp
		st.Status = execstate.StatusRunning
		st.PendingSvc = 0
		st.PendingBody = nil
		n.StateStore.Save(context.Background(), st)
	}

	out, err := n.runSteps(context.Background(), st.ID, st.PlanBytes, names, st.CtxBytes, startResp, st)
	if err != nil {
		slog.Error("recovery: exec failed", "exec_id", st.ID, "error", err)
		st.Status = execstate.StatusFailed
		st.Error = err.Error()
		n.StateStore.Save(context.Background(), st)
		return
	}

	slog.Info("recovery: exec completed", "exec_id", st.ID, "bytes", len(out))
	n.StateStore.Delete(context.Background(), st.ID)
}
