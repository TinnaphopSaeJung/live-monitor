package reporter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"live-monitor/internal/contracts"
)

type HTTPReporter struct {
	baseURL string
	token   string
	client  *http.Client
}

func NewHTTPReporter(
	baseURL string,
	token string,
	timeout time.Duration,
) *HTTPReporter {
	return &HTTPReporter{
		baseURL: strings.TrimRight(
			baseURL,
			"/",
		),
		token: token,
		client: &http.Client{
			Timeout: timeout,
		},
	}
}

func (r *HTTPReporter) SendHeartbeat(
	ctx context.Context,
	heartbeat contracts.Heartbeat,
) error {
	return r.postJSON(
		ctx,
		"/api/v1/agents/heartbeat",
		heartbeat,
	)
}

func (r *HTTPReporter) SendIncident(
	ctx context.Context,
	event contracts.IncidentEvent,
) error {
	return r.postJSON(
		ctx,
		"/api/v1/incidents/events",
		event,
	)
}

func (r *HTTPReporter) postJSON(
	ctx context.Context,
	path string,
	payload any,
) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf(
			"marshal request body: %w",
			err,
		)
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		r.baseURL+path,
		bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf(
			"create request: %w",
			err,
		)
	}

	req.Header.Set(
		"Content-Type",
		"application/json",
	)

	req.Header.Set(
		"Authorization",
		"Bearer "+r.token,
	)

	resp, err := r.client.Do(req)
	if err != nil {
		return fmt.Errorf(
			"send request: %w",
			err,
		)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 &&
		resp.StatusCode < 300 {

		_, _ = io.Copy(
			io.Discard,
			resp.Body,
		)

		return nil
	}

	responseBody, _ := io.ReadAll(
		io.LimitReader(
			resp.Body,
			4096,
		),
	)

	return fmt.Errorf(
		"backend returned status=%d body=%s",
		resp.StatusCode,
		string(responseBody),
	)
}
