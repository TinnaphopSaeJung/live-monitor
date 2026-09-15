ALTER TABLE incident_events
ADD COLUMN line_notified_at TIMESTAMPTZ;

CREATE INDEX idx_incident_events_line_notified_at
ON incident_events(line_notified_at);