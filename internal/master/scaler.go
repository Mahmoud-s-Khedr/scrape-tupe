package master

import (
	"context"
	"log/slog"
)

type Scaler struct {
	logger *slog.Logger
}

func NewScaler(logger *slog.Logger) *Scaler {
	return &Scaler{logger: logger}
}

func (s *Scaler) Run(ctx context.Context) {
	s.logger.Info("scaler not yet implemented")
}
