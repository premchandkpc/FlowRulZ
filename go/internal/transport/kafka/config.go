package kafka

type AcksLevel int

const (
	AcksNone AcksLevel = 0
	AcksOne  AcksLevel = 1
	AcksAll  AcksLevel = -1
)

func AcksLevelFromString(s string) AcksLevel {
	switch s {
	case "0":
		return AcksNone
	case "all", "-1":
		return AcksAll
	default:
		return AcksOne
	}
}

type Config struct {
	Brokers    []string
	GroupID    string
	ConsumerCh chan []byte
	Acks       AcksLevel
	Idempotent bool
}
