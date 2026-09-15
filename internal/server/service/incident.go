package service

import (
	"context"
	"fmt"

	"live-monitor/internal/contracts"
	"live-monitor/internal/server/repository"
)

type IncidentService struct {
	incidentRepository *repository.IncidentRepository
}

type ProcessIncidentResult struct {
	Duplicate bool
}

func NewIncidentService(
	incidentRepository *repository.IncidentRepository,
) *IncidentService {
	return &IncidentService{
		incidentRepository: incidentRepository,
	}
}

func (s *IncidentService) ProcessEvent(
	ctx context.Context,
	event contracts.IncidentEvent,
) (ProcessIncidentResult, error) {
	result, err := s.incidentRepository.ProcessEvent(
		ctx,
		event,
	)
	if err != nil {
		return ProcessIncidentResult{},
			fmt.Errorf(
				"process incident event: %w",
				err,
			)
	}

	return ProcessIncidentResult{
		Duplicate: result.Duplicate,
	}, nil
}
