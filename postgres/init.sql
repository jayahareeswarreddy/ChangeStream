-- Local demo source schema. PostgreSQL starts with wal_level=logical via compose.yaml.
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'dbz';

CREATE TABLE public.orders (
    id BIGSERIAL PRIMARY KEY,
    customer_name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('created', 'paid', 'shipped', 'cancelled')),
    total_cents INTEGER NOT NULL CHECK (total_cents >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO public.orders (customer_name, status, total_cents) VALUES
    ('Asha', 'created', 4599),
    ('Ravi', 'paid', 12900),
    ('Mina', 'shipped', 2750);

GRANT CONNECT ON DATABASE source_db TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON public.orders TO debezium;
GRANT USAGE, SELECT ON SEQUENCE public.orders_id_seq TO debezium;

-- Create the publication as the database administrator. The connector uses it
-- with publication.autocreate.mode=disabled, avoiding publication DDL privileges.
CREATE PUBLICATION cdc_publication FOR TABLE public.orders;

CREATE DATABASE read_model;
\connect read_model

CREATE TABLE orders_read_model (
    id BIGINT PRIMARY KEY,
    customer_name TEXT NOT NULL,
    status TEXT NOT NULL,
    total_cents INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    source_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE processed_events (
    id BIGSERIAL PRIMARY KEY,
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    offset_id BIGINT NOT NULL,
    operation TEXT NOT NULL,
    order_id BIGINT,
    source_ts_ms BIGINT,
    payload JSONB NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (topic, partition_id, offset_id)
);
CREATE INDEX processed_events_processed_at_idx ON processed_events (processed_at DESC);

CREATE TABLE dead_letters (
    id BIGSERIAL PRIMARY KEY,
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    offset_id BIGINT NOT NULL,
    error TEXT NOT NULL,
    raw_value TEXT,
    failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (topic, partition_id, offset_id)
);

CREATE TABLE pipeline_status (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    last_heartbeat TIMESTAMPTZ,
    consumer_lag BIGINT,
    processed_count BIGINT NOT NULL DEFAULT 0,
    failed_count BIGINT NOT NULL DEFAULT 0,
    last_event_at TIMESTAMPTZ,
    last_error TEXT
);
INSERT INTO pipeline_status (id) VALUES (1);
