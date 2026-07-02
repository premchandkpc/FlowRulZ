package memadapter

import (
	"log/slog"

	ports "github.com/premchandkpc/FlowRulZ/server/internal/ports/messaging"
)

func init() {
	ports.RegisterBus("memory", func(cfg ports.Config) (ports.Bus, error) {
		return NewBus(cfg), nil
	})
	slog.Debug("memadapter: registered memory bus")
}
