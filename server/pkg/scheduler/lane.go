package scheduler

type Lane int

const (
	LaneFast Lane = iota
	LaneNormal
	LaneHeavy
)

type LaneConfig struct {
	Lane        Lane
	Concurrency int
	QueueSize   int
}

func DefaultLaneConfigs() map[Lane]LaneConfig {
	return map[Lane]LaneConfig{
		LaneFast:   {Lane: LaneFast, Concurrency: 50, QueueSize: 5000},
		LaneNormal: {Lane: LaneNormal, Concurrency: 20, QueueSize: 2000},
		LaneHeavy:  {Lane: LaneHeavy, Concurrency: 5, QueueSize: 500},
	}
}
