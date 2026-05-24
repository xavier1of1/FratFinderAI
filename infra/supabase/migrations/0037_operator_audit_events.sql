CREATE TABLE IF NOT EXISTS operator_audit_events (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    actor text NOT NULL,
    role text NOT NULL,
    action text NOT NULL,
    route text NOT NULL,
    method text NOT NULL,
    target_type text,
    target_id text,
    request_id text NOT NULL,
    result text NOT NULL CHECK (result IN ('allowed', 'denied', 'error')),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    ip_address text,
    user_agent text,
    error_code text,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS operator_audit_events_created_at_idx
    ON operator_audit_events (created_at DESC);

CREATE INDEX IF NOT EXISTS operator_audit_events_action_result_idx
    ON operator_audit_events (action, result, created_at DESC);

CREATE INDEX IF NOT EXISTS operator_audit_events_target_idx
    ON operator_audit_events (target_type, target_id, created_at DESC);
