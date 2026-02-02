-- Minimal schema for MVP
CREATE TABLE IF NOT EXISTS projects (
    gid VARCHAR(50) PRIMARY KEY,
    name VARCHAR(500) NOT NULL,
    owner_gid VARCHAR(50),
    owner_name VARCHAR(200),
    due_date DATE,
    status VARCHAR(20),
    calculated_progress DECIMAL(5,2),
    last_status_update_at TIMESTAMP,
    last_status_update_by VARCHAR(200),
    last_activity_at TIMESTAMP,
    total_tasks INTEGER,
    completed_tasks INTEGER,
    tasks_created_last_7d INTEGER,
    tasks_completed_last_7d INTEGER,
    raw_data JSONB,
    synced_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS project_changelog (
    id SERIAL PRIMARY KEY,
    project_gid VARCHAR(50) NOT NULL,
    field_name VARCHAR(100) NOT NULL,
    old_value TEXT,
    new_value TEXT,
    changed_at TIMESTAMP,
    detected_at TIMESTAMP NOT NULL,
    sync_id VARCHAR(50),
    FOREIGN KEY (project_gid) REFERENCES projects(gid)
);

CREATE TABLE IF NOT EXISTS findings (
    id SERIAL PRIMARY KEY,
    project_gid VARCHAR(50) NOT NULL,
    rule_id VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL,   -- low, medium, high
    status VARCHAR(20) NOT NULL,     -- open, acknowledged, resolved
    details JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    acknowledged_at TIMESTAMP,
    acknowledged_by VARCHAR(200),
    ack_comment TEXT,
    resolved_at TIMESTAMP,
    FOREIGN KEY (project_gid) REFERENCES projects(gid)
);

CREATE TABLE IF NOT EXISTS sync_log (
    id SERIAL PRIMARY KEY,
    sync_id VARCHAR(50) UNIQUE NOT NULL,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    projects_synced INTEGER,
    changes_detected INTEGER,
    findings_created INTEGER,
    status VARCHAR(20),
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_findings_status ON findings(status);
CREATE INDEX IF NOT EXISTS idx_findings_created ON findings(created_at);
CREATE INDEX IF NOT EXISTS idx_changelog_project ON project_changelog(project_gid);
