package notifier

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"

	"live-monitor/internal/contracts"
)

const linePushURL = "https://api.line.me/v2/bot/message/push"

type LINEClient struct {
	channelAccessToken string
	targetID           string
	client             *http.Client
}

type linePushRequest struct {
	To       string        `json:"to"`
	Messages []lineMessage `json:"messages"`
}

type lineMessage struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

func NewLINEClient(
	channelAccessToken string,
	targetID string,
	timeout time.Duration,
) *LINEClient {
	return &LINEClient{
		channelAccessToken: channelAccessToken,
		targetID:           targetID,

		client: &http.Client{
			Timeout: timeout,
		},
	}
}

func (c *LINEClient) SendIncident(
	ctx context.Context,
	event contracts.IncidentEvent,
) error {
	payload := linePushRequest{
		To: c.targetID,
		Messages: []lineMessage{
			{
				Type: "text",
				Text: buildIncidentMessage(event),
			},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf(
			"marshal LINE request: %w",
			err,
		)
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		linePushURL,
		bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf(
			"create LINE request: %w",
			err,
		)
	}

	req.Header.Set(
		"Content-Type",
		"application/json",
	)

	req.Header.Set(
		"Authorization",
		"Bearer "+c.channelAccessToken,
	)

	// Retry key ต้องเหมือนเดิมสำหรับ Event เดิม
	// event_id เดิม → UUID เดิม
	retryKey := uuid.NewSHA1(
		uuid.NameSpaceOID,
		[]byte(
			"live-monitor-line:"+event.EventID,
		),
	).String()

	req.Header.Set(
		"X-Line-Retry-Key",
		retryKey,
	)

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf(
			"send LINE request: %w",
			err,
		)
	}
	defer resp.Body.Close()

	_, _ = io.Copy(
		io.Discard,
		resp.Body,
	)

	// LINE รับ request สำเร็จ
	if resp.StatusCode >= 200 &&
		resp.StatusCode < 300 {

		return nil
	}

	// Retry key นี้เคยถูก LINE รับสำเร็จแล้ว
	// ถือว่างานของเราสำเร็จเช่นกัน
	if resp.StatusCode == http.StatusConflict {
		return nil
	}

	return fmt.Errorf(
		"LINE returned status=%d",
		resp.StatusCode,
	)
}

func buildIncidentMessage(
	event contracts.IncidentEvent,
) string {
	location := time.FixedZone(
		"Asia/Bangkok",
		7*60*60,
	)

	startedAt := event.StartedAt.
		In(location).
		Format("02/01/2006 15:04:05")

	// --------------------------------------------------
	// Agent Availability
	// --------------------------------------------------

	if event.IncidentType == "AGENT_UNREACHABLE" {
		switch event.EventType {

		case "OPENED":
			return fmt.Sprintf(
				"🚨 Live Monitor Alert\n\n"+
					"เครื่อง: %s\n"+
					"ปัญหา: Monitoring Agent ติดต่อไม่ได้\n"+
					"เริ่มตรวจไม่พบ Heartbeat: %s\n\n"+
					"กรุณาตรวจสอบเครื่อง, Network และ Live Monitor Agent",
				event.MachineID,
				startedAt,
			)

		case "RESOLVED":
			recoveredAt := event.OccurredAt.
				In(location).
				Format("02/01/2006 15:04:05")

			duration := time.Duration(
				valueOrZero(event.DurationMS),
			) * time.Millisecond

			return fmt.Sprintf(
				"✅ Live Monitor Recovered\n\n"+
					"เครื่อง: %s\n"+
					"สถานะ: Monitoring Agent กลับมาติดต่อได้\n"+
					"ระยะเวลาที่ติดต่อไม่ได้: %s\n"+
					"เวลากลับมา: %s",
				event.MachineID,
				duration.Round(
					time.Second,
				),
				recoveredAt,
			)
		}
	}

	// --------------------------------------------------
	// Audio Incidents
	// --------------------------------------------------

	switch event.EventType {

	case "OPENED":
		return fmt.Sprintf(
			"🚨 Live Audio Alert\n\n"+
				"เครื่อง: %s\n"+
				"ปัญหา: %s\n"+
				"เวลาเริ่ม: %s\n\n"+
				"กรุณาตรวจสอบ OBS",
			event.MachineID,
			incidentDisplayName(
				event.IncidentType,
			),
			startedAt,
		)

	case "RESOLVED":
		resolvedAt := event.OccurredAt.
			In(location).
			Format("02/01/2006 15:04:05")

		duration := time.Duration(
			valueOrZero(event.DurationMS),
		) * time.Millisecond

		return fmt.Sprintf(
			"✅ Live Audio Recovered\n\n"+
				"เครื่อง: %s\n"+
				"ปัญหา: %s\n"+
				"สถานะ: กลับมาปกติ\n"+
				"ระยะเวลา: %s\n"+
				"เวลาแก้ไข: %s",
			event.MachineID,
			incidentDisplayName(
				event.IncidentType,
			),
			duration.Round(
				time.Second,
			),
			resolvedAt,
		)
	}

	return fmt.Sprintf(
		"Live Monitor\nMachine: %s\nIncident: %s",
		event.MachineID,
		event.IncidentType,
	)
}

func incidentDisplayName(
	incidentType string,
) string {
	switch incidentType {

	case "AUDIO_MUTED":
		return "ไมโครโฟนถูก Mute (AUDIO_MUTED)"

	case "AUDIO_TOO_LOW":
		return "ระดับเสียงต่ำผิดปกติ (AUDIO_TOO_LOW)"

	case "SIGNAL_LOST":
		return "สัญญาณเสียงหาย (SIGNAL_LOST)"

	case "ROUTING_INVALID":
		return "Audio Track Routing ผิด (ROUTING_INVALID)"

	default:
		return incidentType
	}
}

func valueOrZero(
	value *int64,
) int64 {
	if value == nil {
		return 0
	}

	return *value
}
