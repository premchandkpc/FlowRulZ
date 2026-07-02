package common

type Strategy[T any] interface {
	Name() string
	Execute(ctx T) error
}

type StrategyRegistry[T any] struct {
	strategies map[string]Strategy[T]
	order      []string
}

func NewStrategyRegistry[T any]() *StrategyRegistry[T] {
	return &StrategyRegistry[T]{
		strategies: make(map[string]Strategy[T]),
	}
}

func (r *StrategyRegistry[T]) Register(s Strategy[T]) {
	r.strategies[s.Name()] = s
	r.order = append(r.order, s.Name())
}

func (r *StrategyRegistry[T]) Get(name string) (Strategy[T], bool) {
	s, ok := r.strategies[name]
	return s, ok
}

func (r *StrategyRegistry[T]) ExecuteAll(ctx T) error {
	for _, name := range r.order {
		if err := r.strategies[name].Execute(ctx); err != nil {
			return err
		}
	}
	return nil
}
