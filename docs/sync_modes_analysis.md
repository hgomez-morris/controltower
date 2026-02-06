# Sync Modes Analysis (Full vs Partial)

## Context
We need two sync modes for Asana:
- **Full sync**: all **not completed** projects (current behavior).
- **Partial sync**: only projects that changed since the last full sync.

This note summarizes feasible options, tradeoffs, and implications for the MVP.

---

## Current State Analysis

### Bottleneck Identification
The current full sync makes these API calls per run:
- 1 call to list projects
- N calls to get project details (1 per project)
- N calls to get project tasks (1 per project)

**Example with 50 projects**: 1 + 50 + 50 = **101 API calls**

### Critical Finding: `list_projects` Missing Fields
Current `client.py:list_projects` does NOT request `opt_fields`, so Asana returns only basic fields (`gid`, `name`, `resource_type`).

To enable partial sync, we must request `modified_at`:
```python
# Current (line 19)
opts = {"archived": False, "limit": 100}

# Required for partial sync
opts = {"archived": False, "limit": 100, "opt_fields": "modified_at,name"}
```

---

## Option A — Partial Sync via Project Timestamps

**Approach**
- Store `last_full_sync_at` (timestamp).
- In partial sync:
  - List active projects WITH `modified_at` in opt_fields.
  - Only fetch/process projects where `modified_at > last_full_sync_at`.

**Pros**
- Simple to implement; no extra infrastructure.
- No webhooks or tokens required.
- Works with existing rate-limit/backoff logic.

**Cons**
- `modified_at` may not update for all relevant changes (e.g., task-only changes).
- May miss activity if tasks change but project metadata doesn't.

**Estimated Savings**: 60-80% reduction in API calls (depending on change rate).

---

## Option B — Partial Sync via Asana Events API

**Approach**
- Use the Events API with `sync` token.
- Store `sync_token` and `last_event_at`.
- Partial sync requests return only changed entities since last token.

**Pros**
- True incremental sync: only changed data.
- Scales well with many projects.

**Cons**
- Events API works at **resource level** (per project), NOT workspace level.
- Requires maintaining 1 sync token PER PROJECT.
- Tokens expire if not polled within ~24h.
- Needs fallback logic when tokens invalidate.

**Implications**
- High complexity for MVP.
- Recommended for v2+ if scaling to 500+ projects.

---

## Option C — Partial Sync via Local DB Diff

**Approach**
- Always list all active projects from Asana (with `modified_at`).
- Compare `modified_at` to DB record.
- Only re-fetch projects where timestamp changed.

**Pros**
- No Events API needed.
- More robust than Option A alone.
- Single source of truth (DB) for comparison.

**Cons**
- Still requires listing all projects each sync.
- Savings only from skipping per-project detail fetches.

**Estimated Savings**: 50-70% reduction in API calls.

---

## Option D — Hybrid Sync (RECOMMENDED)

**Approach**
Combine timestamp comparison with staleness checks to catch task-only changes.

**Logic**
```
For each project in list:
  1. If NOT in DB → fetch full (new project)
  2. If modified_at > db.modified_at → fetch full (project changed)
  3. If synced_at > 6 hours old → fetch tasks only (detect task activity)
  4. Else → skip
```

**Pros**
- Catches both project-level and task-level changes.
- Configurable staleness threshold.
- Single code path (no separate "modes" to manage).

**Cons**
- Slightly more complex logic than Option A.
- Still fetches tasks for stale projects even if unchanged.

**Estimated Savings**: 70-85% reduction in API calls.

---

## Recommendation

| Phase | Strategy |
|-------|----------|
| MVP (v1.1) | **Option D (Hybrid)** - Best balance of simplicity and coverage |
| Future (v2) | Consider Option B (Events API) if scaling past 200+ projects |

---

## Implementation Plan

### Database Changes

```sql
-- Add to sync_log table
ALTER TABLE sync_log ADD COLUMN sync_mode VARCHAR(20) DEFAULT 'full';
ALTER TABLE sync_log ADD COLUMN projects_skipped INTEGER DEFAULT 0;
ALTER TABLE sync_log ADD COLUMN projects_partial INTEGER DEFAULT 0;

-- Add config table for sync state
CREATE TABLE IF NOT EXISTS sync_config (
    key VARCHAR(100) PRIMARY KEY,
    value TEXT,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Store last full sync timestamp
INSERT INTO sync_config (key, value) VALUES ('last_full_sync_at', NULL);
```

### Code Changes Summary

| File | Change |
|------|--------|
| `client.py` | Add `opt_fields` to `list_projects` |
| `sync_runner.py` | Add sync mode logic, skip/partial decisions |
| `db/` | Add functions for sync_config table |

---

## Step-by-Step Implementation Prompts

Use these prompts sequentially with an AI assistant to implement the feature.

### Prompt 1: Update Asana Client

```
Context: I'm working on ControlTower, a PMO tool that syncs data from Asana.
File: src/controltower/asana/client.py

Task: Modify the `list_projects` method to include `modified_at` and `name`
in the response by adding opt_fields to the request options.

Current code requests: {"archived": False, "limit": 100}
Should request: {"archived": False, "limit": 100, "opt_fields": "modified_at,name"}

Only modify the opts dict, keep everything else the same.
```

### Prompt 2: Create Database Migration

```
Context: ControlTower project, PostgreSQL database.
File: scripts/migrations/002_sync_modes.sql (create new file)

Task: Create a SQL migration that:
1. Adds columns to sync_log: sync_mode (VARCHAR 20, default 'full'),
   projects_skipped (INTEGER, default 0), projects_partial (INTEGER, default 0)
2. Creates a sync_config table with columns: key (VARCHAR 100, PK),
   value (TEXT), updated_at (TIMESTAMP)
3. Inserts initial row: key='last_full_sync_at', value=NULL

Use IF NOT EXISTS where appropriate for idempotency.
```

### Prompt 3: Add Sync Config DB Functions

```
Context: ControlTower project.
File: src/controltower/db/sync_config.py (create new file)

Task: Create functions to manage sync configuration:
1. get_sync_config(conn, key: str) -> Optional[str] - returns value for key
2. set_sync_config(conn, key: str, value: str) -> None - upserts key/value
3. get_last_full_sync(conn) -> Optional[datetime] - helper that gets
   'last_full_sync_at' and parses to datetime
4. set_last_full_sync(conn, timestamp: datetime) -> None - helper that
   sets 'last_full_sync_at' as ISO string

Use psycopg3 style (the project uses psycopg 3.2.6).
```

### Prompt 4: Add Sync Decision Logic

```
Context: ControlTower project.
File: src/controltower/sync/sync_strategy.py (create new file)

Task: Create a SyncStrategy class with this logic:

class SyncStrategy:
    TASK_REFRESH_HOURS = 6  # Fetch tasks if not synced in this period

    def determine_action(self, asana_project: dict, db_record: Optional[dict],
                         last_full_sync: Optional[datetime]) -> str:
        """
        Returns one of: 'full', 'tasks_only', 'skip'

        Logic:
        - If db_record is None → 'full' (new project)
        - If asana modified_at > db modified_at → 'full' (project changed)
        - If db synced_at older than TASK_REFRESH_HOURS → 'tasks_only' (stale)
        - Else → 'skip'
        """

Include proper datetime parsing and timezone handling (assume UTC).
```

### Prompt 5: Modify Sync Runner

```
Context: ControlTower project.
File: src/controltower/sync/sync_runner.py

Task: Modify main_sync to support partial sync mode:

1. Add parameter: sync_mode: str = 'full' (options: 'full', 'partial')

2. At start of sync:
   - Load last_full_sync_at from sync_config table
   - Initialize counters: skipped=0, partial=0

3. In the project loop, before fetching each project:
   - If sync_mode == 'partial':
     - Get existing DB record for this project
     - Use SyncStrategy.determine_action() to decide
     - If 'skip': increment skipped counter, continue to next project
     - If 'tasks_only': only call get_project_tasks, reuse existing project data
     - If 'full': fetch both project and tasks (current behavior)
   - If sync_mode == 'full': always fetch everything (current behavior)

4. At end of sync:
   - If sync_mode == 'full': update last_full_sync_at to now
   - Update sync_log with sync_mode, projects_skipped, projects_partial counts

Keep backward compatibility - default behavior should match current full sync.
```

### Prompt 6: Update Run Script

```
Context: ControlTower project.
File: scripts/run_sync.py

Task: Add command-line argument to select sync mode:

1. Add argparse with --mode argument (choices: 'full', 'partial', default: 'partial')
2. Pass the mode to main_sync()
3. Print summary at end showing mode used and skip/partial counts

Example usage:
  python scripts/run_sync.py --mode full
  python scripts/run_sync.py --mode partial
  python scripts/run_sync.py  # defaults to partial
```

### Prompt 7: Add Tests

```
Context: ControlTower project.
File: tests/test_sync_strategy.py (create new file)

Task: Write unit tests for SyncStrategy.determine_action():

Test cases:
1. New project (no db_record) → returns 'full'
2. Project modified (asana modified_at > db modified_at) → returns 'full'
3. Stale project (synced_at > 6 hours ago, not modified) → returns 'tasks_only'
4. Fresh project (synced recently, not modified) → returns 'skip'
5. Edge case: db_record has None for modified_at → returns 'full'

Use pytest. Mock datetime.utcnow() for deterministic tests.
```

---

## Validation Checklist

After implementation, verify:

- [ ] `list_projects` returns `modified_at` field
- [ ] Full sync still works as before (backward compatible)
- [ ] Partial sync skips unchanged projects
- [ ] Partial sync catches modified projects
- [ ] Stale projects get task refresh
- [ ] `sync_log` records correct mode and counts
- [ ] `last_full_sync_at` updates after full sync only
- [ ] No regression in rules evaluation or findings

---

## Metrics to Monitor

After deployment, track:

| Metric | Query |
|--------|-------|
| API calls saved | `SELECT SUM(projects_skipped * 2) FROM sync_log WHERE sync_mode='partial'` |
| Partial sync effectiveness | `SELECT AVG(projects_skipped::float / projects_synced) FROM sync_log WHERE sync_mode='partial'` |
| Sync duration comparison | `SELECT sync_mode, AVG(EXTRACT(EPOCH FROM (completed_at - started_at))) FROM sync_log GROUP BY sync_mode` |
