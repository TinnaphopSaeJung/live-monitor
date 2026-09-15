package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"live-monitor/internal/contracts"
	"live-monitor/internal/server/database"
	servermonitor "live-monitor/internal/server/monitor"
	"live-monitor/internal/server/notifier"
	"live-monitor/internal/server/repository"
	"live-monitor/internal/server/service"
	serverworker "live-monitor/internal/server/worker"
)

func main() {
	// --------------------------------------------------
	// 1. Environment Variables
	// --------------------------------------------------

	agentToken := os.Getenv(
		"AGENT_TOKEN",
	)
	if agentToken == "" {
		log.Fatal(
			"AGENT_TOKEN environment variable is required",
		)
	}

	databaseURL := os.Getenv(
		"DATABASE_URL",
	)
	if databaseURL == "" {
		log.Fatal(
			"DATABASE_URL environment variable is required",
		)
	}

	lineChannelAccessToken := os.Getenv(
		"LINE_CHANNEL_ACCESS_TOKEN",
	)
	if lineChannelAccessToken == "" {
		log.Fatal(
			"LINE_CHANNEL_ACCESS_TOKEN environment variable is required",
		)
	}

	lineTargetID := os.Getenv(
		"LINE_TARGET_ID",
	)
	if lineTargetID == "" {
		log.Fatal(
			"LINE_TARGET_ID environment variable is required",
		)
	}

	// --------------------------------------------------
	// 2. Application Context
	// --------------------------------------------------

	ctx := context.Background()

	// --------------------------------------------------
	// 3. PostgreSQL
	// --------------------------------------------------

	db, err := database.NewPostgres(
		ctx,
		databaseURL,
	)
	if err != nil {
		log.Fatalf(
			"connect database: %v",
			err,
		)
	}
	defer db.Close()

	log.Println(
		"Connected to PostgreSQL",
	)

	// --------------------------------------------------
	// 4. Repositories
	// --------------------------------------------------

	machineRepository :=
		repository.NewMachineRepository(
			db,
		)

	incidentRepository :=
		repository.NewIncidentRepository(
			db,
		)

	availabilityRepository :=
		repository.NewAvailabilityRepository(
			db,
		)

	// --------------------------------------------------
	// 5. LINE Client
	// --------------------------------------------------

	lineNotifier := notifier.NewLINEClient(
		lineChannelAccessToken,
		lineTargetID,
		3*time.Second,
	)

	log.Println(
		"LINE notifier configured",
	)

	// --------------------------------------------------
	// 6. Services
	//
	// IMPORTANT:
	//
	// IncidentService ตอนนี้รับผิดชอบแค่:
	//
	// Incident Event
	//      ↓
	// PostgreSQL
	//
	// ไม่ส่ง LINE แล้ว
	// --------------------------------------------------

	incidentService :=
		service.NewIncidentService(
			incidentRepository,
		)

		// --------------------------------------------------
		// 7. Availability Monitor
		//
		// Heartbeat หายเกิน 15s
		//      ↓
		// AGENT_UNREACHABLE
		//
		// Heartbeat กลับมา
		//      ↓
		// HEARTBEAT_RESUMED
		//
		// NOTE:
		// ตอนนี้ AvailabilityMonitor
		// ยังใช้ IncidentRepository โดยตรง
		//
		// Step ถัดไปเราจะ refactor
		// ให้ใช้ IncidentService
		// --------------------------------------------------

	availabilityMonitor := servermonitor.NewAvailabilityMonitor(
		availabilityRepository,
		incidentService,

		15*time.Second,
		5*time.Second,
		15*time.Second,
	)

	// --------------------------------------------------
	// 8. LINE Notification Worker
	//
	// หา:
	//
	// incident_events
	// WHERE line_notified_at IS NULL
	//
	// แล้ว:
	//
	// LINE Push
	//      ↓
	// MarkLineNotified()
	// --------------------------------------------------

	notificationWorker :=
		serverworker.NewNotificationWorker(
			incidentRepository,
			lineNotifier,

			2*time.Second, // check interval

			20, // batch size
		)

	// --------------------------------------------------
	// 9. Start Background Workers
	// --------------------------------------------------

	go availabilityMonitor.Run(
		ctx,
	)

	go notificationWorker.Run(
		ctx,
	)

	// --------------------------------------------------
	// 10. HTTP Router
	// --------------------------------------------------

	mux := http.NewServeMux()

	// --------------------------------------------------
	// Heartbeat
	//
	// Agent
	//   ↓
	// POST /api/v1/agents/heartbeat
	//   ↓
	// MachineRepository
	//   ↓
	// machines
	// --------------------------------------------------

	mux.Handle(
		"POST /api/v1/agents/heartbeat",
		authenticate(
			agentToken,
			handleHeartbeat(
				machineRepository,
			),
		),
	)

	// --------------------------------------------------
	// Incident Event
	//
	// Agent
	//   ↓
	// POST /api/v1/incidents/events
	//   ↓
	// IncidentService
	//   ↓
	// PostgreSQL
	//
	// LINE ไม่ได้อยู่ใน HTTP request path แล้ว
	// NotificationWorker จะส่งภายหลัง
	// --------------------------------------------------

	mux.Handle(
		"POST /api/v1/incidents/events",
		authenticate(
			agentToken,
			handleIncident(
				incidentService,
			),
		),
	)

	// --------------------------------------------------
	// 11. HTTP Server
	// --------------------------------------------------

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,

		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      5 * time.Second,
		IdleTimeout:       30 * time.Second,
	}

	log.Println(
		"Backend listening on :8080",
	)

	// --------------------------------------------------
	// 12. Start HTTP Server
	// --------------------------------------------------

	if err := server.ListenAndServe(); err != nil &&
		!errors.Is(
			err,
			http.ErrServerClosed,
		) {

		log.Fatal(err)
	}
}

// ==================================================
// Heartbeat Handler
//
// POST /api/v1/agents/heartbeat
//
// หน้าที่:
// - Decode
// - Validate
// - UPSERT current machine state
//
// Heartbeat ไม่เกี่ยวกับ LINE โดยตรง
// ==================================================

func handleHeartbeat(
	machineRepository *repository.MachineRepository,
) http.HandlerFunc {
	return func(
		w http.ResponseWriter,
		r *http.Request,
	) {
		var heartbeat contracts.Heartbeat

		// ------------------------------------------
		// Decode
		// ------------------------------------------

		if err := json.NewDecoder(
			r.Body,
		).Decode(&heartbeat); err != nil {

			http.Error(
				w,
				"invalid request body",
				http.StatusBadRequest,
			)

			return
		}

		// ------------------------------------------
		// Validation
		// ------------------------------------------

		if heartbeat.MachineID == "" {
			http.Error(
				w,
				"machine_id is required",
				http.StatusBadRequest,
			)

			return
		}

		if heartbeat.SentAt.IsZero() {
			http.Error(
				w,
				"sent_at is required",
				http.StatusBadRequest,
			)

			return
		}

		if heartbeat.StreamState == "" {
			http.Error(
				w,
				"stream_state is required",
				http.StatusBadRequest,
			)

			return
		}

		// ------------------------------------------
		// PostgreSQL
		// ------------------------------------------

		if err := machineRepository.UpsertHeartbeat(
			r.Context(),
			heartbeat,
		); err != nil {

			log.Printf(
				"HEARTBEAT DB ERROR machine=%s error=%v",
				heartbeat.MachineID,
				err,
			)

			http.Error(
				w,
				"internal server error",
				http.StatusInternalServerError,
			)

			return
		}

		// ------------------------------------------
		// Log
		// ------------------------------------------

		log.Printf(
			"HEARTBEAT machine=%s stream=%s monitoring=%t incidents=%v sent_at=%s",
			heartbeat.MachineID,
			heartbeat.StreamState,
			heartbeat.MonitoringActive,
			heartbeat.ActiveIncidents,
			heartbeat.SentAt.Format(
				time.RFC3339,
			),
		)

		// ------------------------------------------
		// Success
		// ------------------------------------------

		w.WriteHeader(
			http.StatusNoContent,
		)
	}
}

// ==================================================
// Incident Handler
//
// POST /api/v1/incidents/events
//
// หน้าที่:
//
// HTTP Layer
//    ↓
// Validate
//    ↓
// IncidentService
//    ↓
// PostgreSQL
//    ↓
// 204
//
// LINE ถูกแยกออกไปทำใน NotificationWorker
// ==================================================

func handleIncident(
	incidentService *service.IncidentService,
) http.HandlerFunc {
	return func(
		w http.ResponseWriter,
		r *http.Request,
	) {
		var event contracts.IncidentEvent

		// ------------------------------------------
		// Decode
		// ------------------------------------------

		if err := json.NewDecoder(
			r.Body,
		).Decode(&event); err != nil {

			http.Error(
				w,
				"invalid request body",
				http.StatusBadRequest,
			)

			return
		}

		// ------------------------------------------
		// Basic Validation
		// ------------------------------------------

		if event.EventID == "" {
			http.Error(
				w,
				"event_id is required",
				http.StatusBadRequest,
			)

			return
		}

		if event.MachineID == "" {
			http.Error(
				w,
				"machine_id is required",
				http.StatusBadRequest,
			)

			return
		}

		if event.EventType == "" {
			http.Error(
				w,
				"event_type is required",
				http.StatusBadRequest,
			)

			return
		}

		if event.IncidentType == "" {
			http.Error(
				w,
				"incident_type is required",
				http.StatusBadRequest,
			)

			return
		}

		if event.StartedAt.IsZero() {
			http.Error(
				w,
				"started_at is required",
				http.StatusBadRequest,
			)

			return
		}

		if event.OccurredAt.IsZero() {
			http.Error(
				w,
				"occurred_at is required",
				http.StatusBadRequest,
			)

			return
		}

		// ------------------------------------------
		// Event Type Validation
		// ------------------------------------------

		if event.EventType != "OPENED" &&
			event.EventType != "RESOLVED" {

			http.Error(
				w,
				"invalid event_type",
				http.StatusBadRequest,
			)

			return
		}

		// ------------------------------------------
		// RESOLVED Validation
		// ------------------------------------------

		if event.EventType == "RESOLVED" {

			if event.ResolutionReason == nil {
				http.Error(
					w,
					"resolution_reason is required for RESOLVED event",
					http.StatusBadRequest,
				)

				return
			}

			if event.DurationMS == nil {
				http.Error(
					w,
					"duration_ms is required for RESOLVED event",
					http.StatusBadRequest,
				)

				return
			}
		}

		// ------------------------------------------
		// Incident Service
		//
		// Service จะ persist DB เท่านั้น
		//
		// ไม่มี LINE call ตรงนี้แล้ว
		// ------------------------------------------

		result, err := incidentService.ProcessEvent(
			r.Context(),
			event,
		)
		if err != nil {

			log.Printf(
				"INCIDENT PROCESS ERROR event_id=%s machine=%s error=%v",
				event.EventID,
				event.MachineID,
				err,
			)

			http.Error(
				w,
				"internal server error",
				http.StatusInternalServerError,
			)

			return
		}

		// ------------------------------------------
		// Duplicate
		//
		// event_id เคยมีใน PostgreSQL แล้ว
		//
		// ไม่ต้องทำอะไรอีก
		//
		// NotificationWorker จะเป็นคนดู
		// line_notified_at เอง
		// ------------------------------------------

		if result.Duplicate {

			log.Printf(
				"INCIDENT DUPLICATE event_id=%s ignored",
				event.EventID,
			)

			w.WriteHeader(
				http.StatusNoContent,
			)

			return
		}

		// ------------------------------------------
		// New Incident Event
		// ------------------------------------------

		log.Printf(
			"INCIDENT event_id=%s machine=%s event=%s type=%s started_at=%s occurred_at=%s",
			event.EventID,
			event.MachineID,
			event.EventType,
			event.IncidentType,
			event.StartedAt.Format(
				time.RFC3339,
			),
			event.OccurredAt.Format(
				time.RFC3339,
			),
		)

		// ------------------------------------------
		// Resolution Log
		// ------------------------------------------

		if event.ResolutionReason != nil {

			log.Printf(
				"INCIDENT RESOLUTION machine=%s type=%s reason=%s duration_ms=%d",
				event.MachineID,
				event.IncidentType,
				*event.ResolutionReason,
				valueOrZero(
					event.DurationMS,
				),
			)
		}

		// ------------------------------------------
		// สำคัญ:
		//
		// เราตอบ Agent ตรงนี้เลย
		//
		// ไม่ต้องรอ LINE
		// ------------------------------------------

		w.WriteHeader(
			http.StatusNoContent,
		)
	}
}

// ==================================================
// Authentication Middleware
//
// Authorization:
// Bearer <AGENT_TOKEN>
// ==================================================

func authenticate(
	expectedToken string,
	next http.Handler,
) http.Handler {
	return http.HandlerFunc(
		func(
			w http.ResponseWriter,
			r *http.Request,
		) {
			expected := fmt.Sprintf(
				"Bearer %s",
				expectedToken,
			)

			if r.Header.Get(
				"Authorization",
			) != expected {

				http.Error(
					w,
					"unauthorized",
					http.StatusUnauthorized,
				)

				return
			}

			next.ServeHTTP(
				w,
				r,
			)
		},
	)
}

// ==================================================
// Helper
// ==================================================

func valueOrZero(
	value *int64,
) int64 {
	if value == nil {
		return 0
	}

	return *value
}
