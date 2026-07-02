package common

import (
	"context"
	"log/slog"
	"time"
)

type Middleware func(next interface{}) interface{}

type LoggingConfig struct {
	Logger *slog.Logger
}

func LoggingMiddleware(cfg LoggingConfig) Middleware {
	return func(next interface{}) interface{} {
		return next
	}
}

type ServiceMiddleware func(Service) Service

func LoggingService(logger *slog.Logger) ServiceMiddleware {
	return func(next Service) Service {
		return &loggingService{next: next, log: logger}
	}
}

type loggingService struct {
	next Service
	log  *slog.Logger
}

func (s *loggingService) Start(ctx context.Context) error {
	s.log.Info("service: starting")
	start := time.Now()
	err := s.next.Start(ctx)
	s.log.Info("service: started", "duration", time.Since(start), "error", err)
	return err
}

func (s *loggingService) Stop() error {
	s.log.Info("service: stopping")
	start := time.Now()
	err := s.next.Stop()
	s.log.Info("service: stopped", "duration", time.Since(start), "error", err)
	return err
}

func RecoveryMiddleware(next Service) Service {
	return &recoveryService{next: next}
}

type recoveryService struct{ next Service }

func (s *recoveryService) Start(ctx context.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &Error{Class: ClassInternal, Message: "panic in Start", Meta: map[string]any{"panic": r}}
		}
	}()
	return s.next.Start(ctx)
}

func (s *recoveryService) Stop() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &Error{Class: ClassInternal, Message: "panic in Stop", Meta: map[string]any{"panic": r}}
		}
	}()
	return s.next.Stop()
}
