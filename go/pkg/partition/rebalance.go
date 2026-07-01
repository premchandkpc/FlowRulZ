package partition

type RebalanceNotifier interface {
	SetNotify(fn func())
	CheckAndRebalance() bool
}
