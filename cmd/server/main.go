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

	"live-monitor/cmd/server/database"
	"live-monitor/cmd/server/notifier"
	"live-monitor/cmd/server/repository"
	"live-monitor/internal/contracts"
)

func main() {
	// --------------------------------------------------
	// 1. Environment variables
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
	// 2. Database
	// --------------------------------------------------

	ctx := context.Background()

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
	// 3. Repositories
	// --------------------------------------------------

	machineRepository := repository.NewMachineRepository(
		db,
	)

	incidentRepository := repository.NewIncidentRepository(
		db,
	)

	// --------------------------------------------------
	// 4. LINE Notifier
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
	// 5. Router
	// --------------------------------------------------

	mux := http.NewServeMux()

	// Heartbeat
	mux.Handle(
		"POST /api/v1/agents/heartbeat",
		authenticate(
			agentToken,
			handleHeartbeat(
				machineRepository,
			),
		),
	)

	// Incident
	mux.Handle(
		"POST /api/v1/incidents/events",
		authenticate(
			agentToken,
			handleIncident(
				incidentRepository,
				lineNotifier,
			),
		),
	)

	// --------------------------------------------------
	// 6. HTTP Server
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
	// 7. Start Server
	// --------------------------------------------------

	if err := server.ListenAndServe(); err != nil &&
		!errors.Is(
			err,
			http.ErrServerClosed,
		) {

		log.Fatal(err)
	}
}

// --------------------------------------------------
// Heartbeat Handler
//
// Agent
//   ↓
// POST /api/v1/agents/heartbeat
//   ↓
// MachineRepository
//   ↓
// machines
// --------------------------------------------------

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
		// Persist current machine state
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

		w.WriteHeader(
			http.StatusNoContent,
		)
	}
}

// --------------------------------------------------
// Incident Handler
//
// Agent
//   ↓
// Incident Event
//   ↓
// PostgreSQL
//   ↓
// Duplicate check
//   ↓
// LINE
// --------------------------------------------------

func handleIncident(
	incidentRepository *repository.IncidentRepository,
	lineNotifier *notifier.LINEClient,
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
		// Basic validation
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
		// Event type validation
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
		// RESOLVED validation
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
		// PostgreSQL
		//
		// Repository จะ:
		//
		// BEGIN
		//
		// ensure machine
		// insert incident_events
		// update incidents
		//
		// COMMIT
		//
		// และใช้ event_id deduplication
		// ------------------------------------------

		result, err := incidentRepository.ProcessEvent(
			r.Context(),
			event,
		)
		if err != nil {
			log.Printf(
				"INCIDENT DB ERROR event_id=%s machine=%s error=%v",
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
		// Duplicate + LINE เคยส่งแล้ว
		//
		// ไม่ต้องทำอะไรอีก
		//
		// สำคัญ:
		// ตรงนี้ป้องกัน LINE Alert ซ้ำจาก Agent Retry
		// ------------------------------------------

		if result.Duplicate &&
			result.LineNotified {

			log.Printf(
				"INCIDENT DUPLICATE event_id=%s already_notified=true ignored",
				event.EventID,
			)

			w.WriteHeader(
				http.StatusNoContent,
			)

			return
		}

		// ------------------------------------------
		// Event ใหม่
		// ------------------------------------------

		if !result.Duplicate {
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
		}

		// ------------------------------------------
		// Duplicate แต่ LINE ยังไม่เคยสำเร็จ
		//
		// หมายถึง Agent กำลัง Retry
		//
		// เราต้องลอง LINE ต่อ
		// ------------------------------------------

		if result.Duplicate &&
			!result.LineNotified {

			log.Printf(
				"INCIDENT DUPLICATE event_id=%s line_notified=false retrying LINE",
				event.EventID,
			)
		}

		// ------------------------------------------
		// Send LINE
		//
		// LINEClient ใช้ X-Line-Retry-Key
		// จาก event_id แบบ deterministic
		//
		// event_id เดิม
		// → retry key เดิม
		//
		// LINE ป้องกันข้อความซ้ำอีกชั้น
		// ------------------------------------------

		if err := lineNotifier.SendIncident(
			r.Context(),
			event,
		); err != nil {

			log.Printf(
				"LINE NOTIFICATION ERROR event_id=%s machine=%s type=%s error=%v",
				event.EventID,
				event.MachineID,
				event.IncidentType,
				err,
			)

			// --------------------------------------
			// ตอบ error กลับ Agent
			//
			// AsyncReporter ของ Agent จะ Retry
			// event_id เดิม
			// --------------------------------------

			http.Error(
				w,
				"LINE notification failed",
				http.StatusBadGateway,
			)

			return
		}

		// ------------------------------------------
		// LINE สำเร็จ
		//
		// Mark ใน PostgreSQL ว่า Event นี้
		// แจ้ง LINE แล้ว
		// ------------------------------------------

		if err := incidentRepository.MarkLineNotified(
			r.Context(),
			event.EventID,
		); err != nil {

			log.Printf(
				"LINE NOTIFICATION DB ERROR event_id=%s machine=%s error=%v",
				event.EventID,
				event.MachineID,
				err,
			)

			// --------------------------------------
			// LINE อาจถูกส่งไปแล้ว
			//
			// แต่ mark DB ไม่สำเร็จ
			//
			// เราตอบ 500 ให้ Agent Retry
			//
			// LINE Retry Key จะช่วยไม่ให้ Push ซ้ำ
			// --------------------------------------

			http.Error(
				w,
				"internal server error",
				http.StatusInternalServerError,
			)

			return
		}

		log.Printf(
			"LINE NOTIFICATION SENT event_id=%s machine=%s type=%s event=%s",
			event.EventID,
			event.MachineID,
			event.IncidentType,
			event.EventType,
		)

		// ------------------------------------------
		// Success
		// ------------------------------------------

		w.WriteHeader(
			http.StatusNoContent,
		)
	}
}

// --------------------------------------------------
// Bearer Token Authentication
//
// Agent:
//
// Authorization: Bearer <AGENT_TOKEN>
// --------------------------------------------------

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

// --------------------------------------------------
// Helper
// --------------------------------------------------

func valueOrZero(
	value *int64,
) int64 {
	if value == nil {
		return 0
	}

	return *value
}
