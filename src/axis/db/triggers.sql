-- SQL helper: create notify function and example trigger.
-- Replace table name or use templated deployment to attach triggers to multiple tables.

CREATE TABLE IF NOT EXISTS axis_events (
    id serial PRIMARY KEY,
    occurred_at timestamptz DEFAULT now(),
    table_name text NOT NULL,
    operation text NOT NULL,
    payload jsonb
);

CREATE OR REPLACE FUNCTION notify_row_change() RETURNS trigger AS $$
DECLARE
    data jsonb;
BEGIN
    IF (TG_OP = 'DELETE') THEN
        data := row_to_json(OLD)::jsonb;
    ELSE
        data := row_to_json(NEW)::jsonb;
    END IF;

    INSERT INTO axis_events (table_name, operation, payload)
    VALUES (TG_TABLE_NAME, TG_OP, data);

    PERFORM pg_notify(current_setting('axis.notify_channel', true)::text, json_build_object(
        'table', TG_TABLE_NAME,
        'operation', TG_OP,
        'payload', data
    )::text);

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Example attaching trigger (adjust as needed):
-- CREATE TRIGGER users_changed AFTER INSERT OR UPDATE OR DELETE ON users
-- FOR EACH ROW EXECUTE PROCEDURE notify_row_change();