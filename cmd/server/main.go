package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"live-monitor/internal/contracts"
)

// --------------------------------------------------
// In-memory incident event deduplication
//
// ใช้สำหรับ prototype ก่อน
//
// event_id เดิมที่ Retry เข้ามาซ้ำ
// จะไม่ถูก process ซ้ำ
//
// หมายเหตุ:
// ข้อมูลนี้หายเมื่อ Server restart
// ภายหลัง PostgreSQL จะใช้ UNIQUE(event_id)
// --------------------------------------------------

var seenIncidentEvents sync.Map

func main() {
	// --------------------------------------------------
	// 1. Load Agent Token
	// --------------------------------------------------

	token := os.Getenv(
		"AGENT_TOKEN",
	)

	if token == "" {
		log.Fatal(
			"AGENT_TOKEN environment variable is required",
		)
	}

	// --------------------------------------------------
	// 2. HTTP Router
	// --------------------------------------------------

	mux := http.NewServeMux()

	mux.Handle(
		"POST /api/v1/agents/heartbeat",
		authenticate(
			token,
			http.HandlerFunc(
				handleHeartbeat,
			),
		),
	)

	mux.Handle(
		"POST /api/v1/incidents/events",
		authenticate(
			token,
			http.HandlerFunc(
				handleIncident,
			),
		),
	)

	// --------------------------------------------------
	// 3. HTTP Server
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
	// 4. Start Server
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
// Endpoint:
// POST /api/v1/agents/heartbeat
//
// หน้าที่:
// รับ current snapshot จาก Agent
//
// ตอนนี้:
// log ออก terminal
//
// ต่อไป:
// update machine status / last_seen ใน DB
// --------------------------------------------------

func handleHeartbeat(
	w http.ResponseWriter,
	r *http.Request,
) {
	var heartbeat contracts.Heartbeat

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

	// --------------------------------------------------
	// Basic validation
	// --------------------------------------------------

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

	// --------------------------------------------------
	// Prototype processing
	// --------------------------------------------------

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

	// --------------------------------------------------
	// 204 No Content
	//
	// Agent ไม่ต้องการ response body
	// แค่รู้ว่า Backend รับสำเร็จ
	// --------------------------------------------------

	w.WriteHeader(
		http.StatusNoContent,
	)
}

// --------------------------------------------------
// Incident Handler
//
// Endpoint:
// POST /api/v1/incidents/events
//
// รับทั้ง:
// OPENED
// RESOLVED
//
// ตอนนี้:
// - validate
// - deduplicate ด้วย event_id
// - log
//
// ต่อไป:
// - persist DB
// - trigger LINE
// --------------------------------------------------

func handleIncident(
	w http.ResponseWriter,
	r *http.Request,
) {
	var event contracts.IncidentEvent

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

	// --------------------------------------------------
	// Validation
	// --------------------------------------------------

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

	// --------------------------------------------------
	// Deduplication
	//
	// Retry จาก Agent จะใช้ event_id เดิม
	//
	// ถ้าเคยรับ event_id นี้แล้ว:
	// - ไม่ process ซ้ำ
	// - ตอบ 204
	//
	// สำคัญ:
	// เราตอบ success เพื่อให้ Agent หยุด Retry
	// --------------------------------------------------

	if _, loaded := seenIncidentEvents.LoadOrStore(
		event.EventID,
		struct{}{},
	); loaded {

		log.Printf(
			"INCIDENT DUPLICATE event_id=%s ignored",
			event.EventID,
		)

		w.WriteHeader(
			http.StatusNoContent,
		)

		return
	}

	// --------------------------------------------------
	// Prototype Incident Processing
	// --------------------------------------------------

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

	// --------------------------------------------------
	// Resolution information
	// --------------------------------------------------

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

	w.WriteHeader(
		http.StatusNoContent,
	)
}

// --------------------------------------------------
// Bearer Token Authentication
//
// Request:
//
// Authorization: Bearer <token>
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
