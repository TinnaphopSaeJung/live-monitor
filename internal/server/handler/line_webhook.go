package handler

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"log"
	"net/http"
)

const maxLINEWebhookBodySize = 1 << 20 // 1 MB

type LINEWebhookHandler struct {
	channelSecret string
}

type lineWebhookPayload struct {
	Destination string             `json:"destination"`
	Events      []lineWebhookEvent `json:"events"`
}

type lineWebhookEvent struct {
	Type   string            `json:"type"`
	Source lineWebhookSource `json:"source"`
}

type lineWebhookSource struct {
	Type    string `json:"type"`
	UserID  string `json:"userId"`
	GroupID string `json:"groupId"`
	RoomID  string `json:"roomId"`
}

func NewLINEWebhookHandler(
	channelSecret string,
) *LINEWebhookHandler {
	return &LINEWebhookHandler{
		channelSecret: channelSecret,
	}
}

func (h *LINEWebhookHandler) ServeHTTP(
	w http.ResponseWriter,
	r *http.Request,
) {
	if r.Method != http.MethodPost {
		http.Error(
			w,
			"method not allowed",
			http.StatusMethodNotAllowed,
		)
		return
	}

	// --------------------------------------------------
	// 1. Read raw request body
	//
	// สำคัญ:
	// ต้อง verify signature จาก raw body
	// ก่อน json.Unmarshal
	// --------------------------------------------------

	r.Body = http.MaxBytesReader(
		w,
		r.Body,
		maxLINEWebhookBodySize,
	)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(
			w,
			"invalid request body",
			http.StatusBadRequest,
		)
		return
	}

	// --------------------------------------------------
	// 2. Verify LINE signature
	// --------------------------------------------------

	signature := r.Header.Get(
		"X-Line-Signature",
	)

	if signature == "" {
		http.Error(
			w,
			"missing LINE signature",
			http.StatusUnauthorized,
		)
		return
	}

	if !verifyLINESignature(
		body,
		signature,
		h.channelSecret,
	) {
		http.Error(
			w,
			"invalid LINE signature",
			http.StatusUnauthorized,
		)
		return
	}

	// --------------------------------------------------
	// 3. Decode webhook JSON
	// --------------------------------------------------

	var payload lineWebhookPayload

	if err := json.Unmarshal(
		body,
		&payload,
	); err != nil {
		http.Error(
			w,
			"invalid JSON",
			http.StatusBadRequest,
		)
		return
	}

	// --------------------------------------------------
	// 4. Inspect events
	// --------------------------------------------------

	for _, event := range payload.Events {
		switch event.Source.Type {

		case "group":
			log.Printf(
				"LINE WEBHOOK event=%s source=group group_id=%s user_id=%s",
				event.Type,
				event.Source.GroupID,
				event.Source.UserID,
			)

		case "user":
			log.Printf(
				"LINE WEBHOOK event=%s source=user user_id=%s",
				event.Type,
				event.Source.UserID,
			)

		case "room":
			log.Printf(
				"LINE WEBHOOK event=%s source=room room_id=%s user_id=%s",
				event.Type,
				event.Source.RoomID,
				event.Source.UserID,
			)
		}
	}

	// LINE ต้องการ HTTP success response
	w.WriteHeader(
		http.StatusOK,
	)
}

func verifyLINESignature(
	body []byte,
	signature string,
	channelSecret string,
) bool {
	receivedSignature, err :=
		base64.StdEncoding.DecodeString(
			signature,
		)

	if err != nil {
		return false
	}

	mac := hmac.New(
		sha256.New,
		[]byte(channelSecret),
	)

	_, _ = mac.Write(body)

	expectedSignature := mac.Sum(nil)

	return hmac.Equal(
		receivedSignature,
		expectedSignature,
	)
}
