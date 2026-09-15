package worker

import (
	"context"
	"log"
	"time"

	"live-monitor/internal/server/notifier"
	"live-monitor/internal/server/repository"
)

type NotificationWorker struct {
	incidentRepository *repository.IncidentRepository
	lineNotifier       *notifier.LINEClient

	checkInterval time.Duration
	batchSize     int
}

func NewNotificationWorker(
	incidentRepository *repository.IncidentRepository,
	lineNotifier *notifier.LINEClient,
	checkInterval time.Duration,
	batchSize int,
) *NotificationWorker {
	return &NotificationWorker{
		incidentRepository: incidentRepository,
		lineNotifier:       lineNotifier,
		checkInterval:      checkInterval,
		batchSize:          batchSize,
	}
}

func (w *NotificationWorker) Run(
	ctx context.Context,
) {
	log.Printf(
		"LINE notification worker started interval=%s batch_size=%d",
		w.checkInterval,
		w.batchSize,
	)

	// ตอน Backend start ให้ตรวจ pending เดิมทันที
	w.processPending(ctx)

	ticker := time.NewTicker(
		w.checkInterval,
	)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			w.processPending(ctx)
		}
	}
}

func (w *NotificationWorker) processPending(
	ctx context.Context,
) {
	events, err :=
		w.incidentRepository.FindPendingLineNotifications(
			ctx,
			w.batchSize,
		)

	if err != nil {
		log.Printf(
			"LINE WORKER QUERY ERROR: %v",
			err,
		)

		return
	}

	for _, event := range events {
		// ------------------------------------------
		// LINE Push
		//
		// LINEClient ของเรามี X-Line-Retry-Key
		// deterministic จาก event_id อยู่แล้ว
		// ------------------------------------------

		if err := w.lineNotifier.SendIncident(
			ctx,
			event,
		); err != nil {

			log.Printf(
				"LINE NOTIFICATION ERROR event_id=%s machine=%s type=%s event=%s error=%v",
				event.EventID,
				event.MachineID,
				event.IncidentType,
				event.EventType,
				err,
			)

			// ไม่ mark DB
			//
			// รอบถัดไป Worker จะเจอ
			// line_notified_at=NULL อีกครั้ง
			continue
		}

		// ------------------------------------------
		// LINE success
		// ------------------------------------------

		if err := w.incidentRepository.MarkLineNotified(
			ctx,
			event.EventID,
		); err != nil {

			log.Printf(
				"LINE NOTIFICATION DB ERROR event_id=%s machine=%s error=%v",
				event.EventID,
				event.MachineID,
				err,
			)

			// LINE อาจส่งสำเร็จไปแล้ว
			//
			// รอบถัดไป X-Line-Retry-Key เดิม
			// จะช่วยป้องกันข้อความซ้ำ
			continue
		}

		log.Printf(
			"LINE NOTIFICATION SENT event_id=%s machine=%s type=%s event=%s",
			event.EventID,
			event.MachineID,
			event.IncidentType,
			event.EventType,
		)
	}
}
