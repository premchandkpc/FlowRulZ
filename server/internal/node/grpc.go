package node

import "log/slog"

func (n *ProdNode) startGRPC() {
	if n.GRPCBus == nil {
		return
	}
	if err := n.GRPCBus.Start(); err != nil {
		slog.Error("grpc: start error", "error", err)
	}
}
