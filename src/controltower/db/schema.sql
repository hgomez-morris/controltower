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
    tasks_modified_last_7d INTEGER,
    pmo_id VARCHAR(100),
    sponsor VARCHAR(200),
    responsable_proyecto VARCHAR(200),
    business_vertical VARCHAR(200),
    fase_proyecto VARCHAR(200),
    en_plan_facturacion BOOLEAN,
    completed_flag BOOLEAN,
    raw_data JSONB,
    synced_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

ALTER TABLE projects ADD COLUMN IF NOT EXISTS tasks_modified_last_7d INTEGER;
ALTER TABLE projects ADD COLUMN IF NOT EXISTS pmo_id VARCHAR(100);
ALTER TABLE projects ADD COLUMN IF NOT EXISTS sponsor VARCHAR(200);
ALTER TABLE projects ADD COLUMN IF NOT EXISTS responsable_proyecto VARCHAR(200);
ALTER TABLE projects ADD COLUMN IF NOT EXISTS business_vertical VARCHAR(200);
ALTER TABLE projects ADD COLUMN IF NOT EXISTS fase_proyecto VARCHAR(200);
ALTER TABLE projects ADD COLUMN IF NOT EXISTS en_plan_facturacion BOOLEAN;
ALTER TABLE projects ADD COLUMN IF NOT EXISTS completed_flag BOOLEAN;

CREATE INDEX IF NOT EXISTS idx_projects_pmo_id ON projects(pmo_id);
CREATE INDEX IF NOT EXISTS idx_projects_sponsor ON projects(sponsor);
CREATE INDEX IF NOT EXISTS idx_projects_business_vertical ON projects(business_vertical);
CREATE INDEX IF NOT EXISTS idx_projects_fase ON projects(fase_proyecto);
CREATE INDEX IF NOT EXISTS idx_projects_en_plan_facturacion ON projects(en_plan_facturacion);

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

CREATE TABLE IF NOT EXISTS status_updates (
    gid VARCHAR(50) PRIMARY KEY,
    project_gid VARCHAR(50) NOT NULL,
    created_at TIMESTAMP,
    author_gid VARCHAR(50),
    author_name VARCHAR(200),
    status_type VARCHAR(50),
    title TEXT,
    text TEXT,
    html_text TEXT,
    raw_data JSONB,
    synced_at TIMESTAMP NOT NULL,
    FOREIGN KEY (project_gid) REFERENCES projects(gid)
);

CREATE TABLE IF NOT EXISTS status_update_comments (
    id SERIAL PRIMARY KEY,
    status_update_gid VARCHAR(50) NOT NULL,
    story_gid VARCHAR(50) NOT NULL,
    created_at TIMESTAMP,
    author_gid VARCHAR(50),
    author_name VARCHAR(200),
    text TEXT,
    html_text TEXT,
    raw_data JSONB,
    synced_at TIMESTAMP NOT NULL,
    UNIQUE (story_gid),
    FOREIGN KEY (status_update_gid) REFERENCES status_updates(gid)
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

CREATE TABLE IF NOT EXISTS kpi_snapshots (
    id SERIAL PRIMARY KEY,
    kpi_id VARCHAR(50) NOT NULL,
    scope_type VARCHAR(20) NOT NULL,   -- empresa, sponsor, jp
    scope_value VARCHAR(200) NOT NULL, -- nombre de sponsor/jp o 'Empresa'
    as_of TIMESTAMP NOT NULL,
    total_projects INTEGER NOT NULL,
    compliant_projects INTEGER NOT NULL,
    kpi_value DECIMAL(5,2) NOT NULL
);

CREATE TABLE IF NOT EXISTS projects_history (
    gid VARCHAR(50) PRIMARY KEY,
    name VARCHAR(500),
    owner_gid VARCHAR(50),
    owner_name VARCHAR(200),
    status VARCHAR(20),
    last_status_update_at TIMESTAMP,
    last_status_update_by VARCHAR(200),
    pmo_id VARCHAR(100),
    cliente_nuevo VARCHAR(200),
    responsable_proyecto VARCHAR(200),
    sponsor VARCHAR(200),
    aws_opp_id VARCHAR(200),
    id_comercial VARCHAR(200),
    search_text TEXT,
    raw_data JSONB,
    snapshot_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS payments (
    id SERIAL PRIMARY KEY,
    project_gid VARCHAR(50),
    pmo_id VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL, -- estimado | efectuado
    payment_date DATE NOT NULL,
    glosa TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS payment_estimate_history (
    id SERIAL PRIMARY KEY,
    payment_id INTEGER NOT NULL,
    old_date DATE,
    new_date DATE,
    changed_at TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (payment_id) REFERENCES payments(id)
);

CREATE INDEX IF NOT EXISTS idx_payments_pmo_id ON payments(pmo_id);
CREATE INDEX IF NOT EXISTS idx_payments_project ON payments(project_gid);

CREATE INDEX IF NOT EXISTS idx_findings_status ON findings(status);
CREATE INDEX IF NOT EXISTS idx_findings_created ON findings(created_at);
CREATE INDEX IF NOT EXISTS idx_changelog_project ON project_changelog(project_gid);
CREATE INDEX IF NOT EXISTS idx_kpi_snapshots ON kpi_snapshots(kpi_id, scope_type, scope_value, as_of);
CREATE INDEX IF NOT EXISTS idx_projects_history_search ON projects_history(search_text);
