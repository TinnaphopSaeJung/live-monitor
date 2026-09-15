CREATE TABLE machines (
    machine_id TEXT PRIMARY KEY,

    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    agent_sent_at TIMESTAMPTZ,

    obs_connected BOOLEAN NOT NULL DEFAULT FALSE,

    stream_state TEXT NOT NULL,
    monitoring_active BOOLEAN NOT NULL DEFAULT FALSE,

    signal_state TEXT NOT NULL,
    level_state TEXT NOT NULL,
    mute_state TEXT NOT NULL,
    routing_state TEXT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE TABLE incidents (
    id BIGSERIAL PRIMARY KEY,

    machine_id TEXT NOT NULL
        REFERENCES machines(machine_id),

    incident_type TEXT NOT NULL,

    status TEXT NOT NULL,

    started_at TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ,

    resolution_reason TEXT,
    duration_ms BIGINT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (
        machine_id,
        incident_type,
        started_at
    )
);


CREATE TABLE incident_events (
    event_id TEXT PRIMARY KEY,

    machine_id TEXT NOT NULL
        REFERENCES machines(machine_id),

    event_type TEXT NOT NULL,
    incident_type TEXT NOT NULL,

    started_at TIMESTAMPTZ NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,

    resolution_reason TEXT,
    duration_ms BIGINT,

    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE INDEX idx_incidents_machine_id
    ON incidents(machine_id);


CREATE INDEX idx_incidents_status
    ON incidents(status);


CREATE INDEX idx_incident_events_machine_id
    ON incident_events(machine_id);


CREATE INDEX idx_incident_events_occurred_at
    ON incident_events(occurred_at);

ALTER TABLE public.machines
ENABLE ROW LEVEL SECURITY;

ALTER TABLE public.incidents
ENABLE ROW LEVEL SECURITY;

ALTER TABLE public.incident_events
ENABLE ROW LEVEL SECURITY;