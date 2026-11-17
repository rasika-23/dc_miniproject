-- Node-local event journal. Every node (and the experiment controller) owns its own file.
-- Nothing here is replicated: databases are collected and merged by event_id when needed.

CREATE TABLE IF NOT EXISTS node_events (
    event_id        TEXT PRIMARY KEY,
    ts              REAL    NOT NULL,
    node_id         INTEGER NOT NULL,
    incarnation     INTEGER NOT NULL,
    event_type      TEXT    NOT NULL,
    epoch           INTEGER NOT NULL DEFAULT 0,
    subject_node_id INTEGER,
    details         TEXT
);
CREATE INDEX IF NOT EXISTS idx_node_events_ts ON node_events (ts);
CREATE INDEX IF NOT EXISTS idx_node_events_type ON node_events (event_type);

CREATE TABLE IF NOT EXISTS election_events (
    event_id    TEXT PRIMARY KEY,
    ts          REAL    NOT NULL,
    node_id     INTEGER NOT NULL,
    incarnation INTEGER NOT NULL,
    event_type  TEXT    NOT NULL,
    epoch       INTEGER NOT NULL DEFAULT 0,
    election_id TEXT,
    duration_ms REAL,
    details     TEXT
);
CREATE INDEX IF NOT EXISTS idx_election_events_ts ON election_events (ts);

CREATE TABLE IF NOT EXISTS fault_experiments (
    fault_id       TEXT PRIMARY KEY,
    experiment_id  TEXT,
    run_index      INTEGER,
    scenario       TEXT,
    fault_type     TEXT    NOT NULL,
    target_node_id INTEGER,
    params         TEXT,
    start_ts       REAL    NOT NULL,
    end_ts         REAL,
    source         TEXT
);
CREATE INDEX IF NOT EXISTS idx_fault_start ON fault_experiments (start_ts);

CREATE TABLE IF NOT EXISTS metric_results (
    result_id     TEXT PRIMARY KEY,
    experiment_id TEXT    NOT NULL,
    run_index     INTEGER NOT NULL,
    scenario      TEXT    NOT NULL,
    metric_name   TEXT    NOT NULL,
    value         REAL,
    unit          TEXT,
    ts            REAL    NOT NULL,
    details       TEXT
);
CREATE INDEX IF NOT EXISTS idx_metric_exp ON metric_results (experiment_id);
