package reporter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"live-monitor/internal/contracts"
)

var ErrIncidentQueueFull = errors.New(
	"incident report queue is full",
)

type AsyncConfig struct {
	IncidentQueueSize     int
	IncidentRetryAttempts int

	RetryInitialBackoff time.Duration
	RetryMaxBackoff     time.Duration
}

type AsyncReporter struct {
	inner Reporter

	incidentQueue  chan contracts.IncidentEvent
	heartbeatQueue chan contracts.Heartbeat

	errors chan error

	config AsyncConfig
}

func NewAsyncReporter(
	inner Reporter,
	config AsyncConfig,
) *AsyncReporter {
	if config.IncidentQueueSize <= 0 {
		config.IncidentQueueSize = 64
	}

	if config.IncidentRetryAttempts <= 0 {
		config.IncidentRetryAttempts = 5
	}

	if config.RetryInitialBackoff <= 0 {
		config.RetryInitialBackoff = 500 * time.Millisecond
	}

	if config.RetryMaxBackoff <= 0 {
		config.RetryMaxBackoff = 5 * time.Second
	}

	return &AsyncReporter{
		inner: inner,

		incidentQueue: make(
			chan contracts.IncidentEvent,
			config.IncidentQueueSize,
		),

		// Heartbeat สนใจเฉพาะ state ล่าสุด
		heartbeatQueue: make(
			chan contracts.Heartbeat,
			1,
		),

		errors: make(
			chan error,
			32,
		),

		config: config,
	}
}

func (r *AsyncReporter) Run(
	ctx context.Context,
) {
	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()

		r.runIncidentWorker(
			ctx,
		)
	}()

	go func() {
		defer wg.Done()

		r.runHeartbeatWorker(
			ctx,
		)
	}()

	wg.Wait()
}

func (r *AsyncReporter) Errors() <-chan error {
	return r.errors
}

// --------------------------------------------------
// SendIncident
//
// Main loop แค่ enqueue
// ไม่ทำ HTTP ตรงนี้
// --------------------------------------------------

func (r *AsyncReporter) SendIncident(
	ctx context.Context,
	event contracts.IncidentEvent,
) error {
	select {

	case <-ctx.Done():
		return ctx.Err()

	case r.incidentQueue <- event:
		return nil

	default:
		return ErrIncidentQueueFull
	}
}

// --------------------------------------------------
// SendHeartbeat
//
// Heartbeat ต่างจาก Incident:
//
// ถ้ามี heartbeat เก่ารออยู่ เราไม่จำเป็นต้องส่งมันแล้ว
// เราสนใจ current state ล่าสุดเท่านั้น
//
// ดังนั้น queue มีแค่ 1 slot และ latest wins
// --------------------------------------------------

func (r *AsyncReporter) SendHeartbeat(
	ctx context.Context,
	heartbeat contracts.Heartbeat,
) error {
	select {
	case <-ctx.Done():
		return ctx.Err()

	default:
	}

	// ลองใส่ทันที
	select {
	case r.heartbeatQueue <- heartbeat:
		return nil

	default:
	}

	// Queue เต็ม → ทิ้ง Heartbeat เก่า
	select {
	case <-r.heartbeatQueue:

	default:
	}

	// ใส่ตัวล่าสุดแทน
	select {
	case r.heartbeatQueue <- heartbeat:

	default:
	}

	return nil
}

// --------------------------------------------------
// Incident Worker
// --------------------------------------------------

func (r *AsyncReporter) runIncidentWorker(
	ctx context.Context,
) {
	for {
		select {

		case <-ctx.Done():
			return

		case event := <-r.incidentQueue:
			r.deliverIncident(
				ctx,
				event,
			)
		}
	}
}

// --------------------------------------------------
// Heartbeat Worker
//
// Heartbeat ไม่ Retry แบบ Incident
//
// เพราะอีก 5 วินาทีก็มี Snapshot ใหม่อยู่แล้ว
// การ Retry heartbeat เก่าอาจส่ง stale state
// --------------------------------------------------

func (r *AsyncReporter) runHeartbeatWorker(
	ctx context.Context,
) {
	for {
		select {

		case <-ctx.Done():
			return

		case heartbeat := <-r.heartbeatQueue:
			if err := r.inner.SendHeartbeat(
				ctx,
				heartbeat,
			); err != nil {

				r.emitError(
					fmt.Errorf(
						"heartbeat delivery failed machine=%s: %w",
						heartbeat.MachineID,
						err,
					),
				)
			}
		}
	}
}

// --------------------------------------------------
// Incident Delivery + Retry
// --------------------------------------------------

func (r *AsyncReporter) deliverIncident(
	ctx context.Context,
	event contracts.IncidentEvent,
) {
	backoff := r.config.RetryInitialBackoff

	for attempt := 1; attempt <= r.config.IncidentRetryAttempts; attempt++ {

		err := r.inner.SendIncident(
			ctx,
			event,
		)

		if err == nil {
			return
		}

		r.emitError(
			fmt.Errorf(
				"incident delivery failed event_id=%s type=%s event=%s attempt=%d/%d: %w",
				event.EventID,
				event.IncidentType,
				event.EventType,
				attempt,
				r.config.IncidentRetryAttempts,
				err,
			),
		)

		if attempt ==
			r.config.IncidentRetryAttempts {

			return
		}

		timer := time.NewTimer(
			backoff,
		)

		select {

		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}

			return

		case <-timer.C:
		}

		backoff *= 2

		if backoff >
			r.config.RetryMaxBackoff {

			backoff =
				r.config.RetryMaxBackoff
		}
	}
}

// --------------------------------------------------
// Async Error Channel
// --------------------------------------------------

func (r *AsyncReporter) emitError(
	err error,
) {
	select {
	case r.errors <- err:

	default:
		// ไม่ยอมให้ error logging
		// block reporting worker
	}
}
