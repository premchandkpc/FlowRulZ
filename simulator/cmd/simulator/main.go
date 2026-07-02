package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/premchandkpc/FlowRulZ/simulator"
	"github.com/premchandkpc/FlowRulZ/simulator/config"
	"github.com/premchandkpc/FlowRulZ/simulator/network"
	"github.com/premchandkpc/FlowRulZ/simulator/scenarios"
)

func main() {
	nodes := flag.Int("nodes", 3, "number of execution nodes")
	workers := flag.Int("workers", 4, "workers per node")
	scenario := flag.String("scenario", "black-friday", "scenario name")
	rate := flag.Int("rate", 0, "requests per second (overrides scenario)")
	duration := flag.Duration("duration", 0, "test duration (overrides scenario)")
	speed := flag.Float64("speed", 1.0, "simulation speed multiplier")
	dash := flag.Bool("dashboard", true, "enable web dashboard")
	dashAddr := flag.String("dashboard-addr", ":8081", "dashboard listen address")
	drop := flag.Bool("drop", false, "enable packet dropping")
	slow := flag.Bool("slow", false, "enable slow network")
	scenariosFlag := flag.Bool("scenarios", false, "list available scenarios")
	verbose := flag.Bool("verbose", false, "verbose output")
	interactive := flag.Bool("interactive", false, "interactive mode with admin API (no auto-stop)")

	flag.Parse()

	if *scenariosFlag {
		fmt.Println("Available scenarios:")
		for _, s := range scenarios.All {
			fmt.Printf("  %-16s %s\n", s.Name, s.Description)
		}
		os.Exit(0)
	}

	if !*interactive && *scenario != "" && scenarios.ByName(*scenario) == nil {
		fmt.Fprintf(os.Stderr, "unknown scenario: %s\n", *scenario)
		fmt.Fprintf(os.Stderr, "use --scenarios to list available\n")
		os.Exit(1)
	}

	cfg := config.SimConfig{
		Nodes:         *nodes,
		Workers:       *workers,
		Scenario:      *scenario,
		Duration:      *duration,
		Rate:          *rate,
		Speed:         *speed,
		Dashboard:     *dash,
		DashboardAddr: *dashAddr,
		Verbose:       *verbose,
	}

	if *drop || *slow {
		cfg.Chaos = network.ChaosConfig{
			DropPackets:  *drop,
			SlowNetwork:  *slow,
			SlowFactor:   3.0,
			DuplicatePct: 1.0,
		}
	}

	if *interactive {
		sim := simulator.New(cfg)
		sim.RegisterAdminHandlers()

		ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer cancel()

		sim.Dispatcher.StartAll()
		if cfg.Dashboard {
			sim.Dashboard.Start()
		}
		slog.Info("simulator: interactive mode", "addr", cfg.DashboardAddr)
		<-ctx.Done()
		slog.Info("shutting down...")
		sim.Stop()
		return
	}

	sim := simulator.New(cfg)
	if err := sim.Run(); err != nil {
		slog.Error("simulator error", "error", err)
		os.Exit(1)
	}
}
