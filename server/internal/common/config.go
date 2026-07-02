package common

type Validator interface {
	Validate() error
}

func MustValidate(v Validator) {
	if err := v.Validate(); err != nil {
		panic("config validation failed: " + err.Error())
	}
}

type ConfigOption[T any] func(*T)

func ApplyOptions[T any](cfg *T, opts ...ConfigOption[T]) *T {
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}
