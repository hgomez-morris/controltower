# Consolidation Analysis: ControlTower + BOTPMO

## Executive Summary

Two independent systems (ControlTower and BOTPMO) currently sync data from the same Asana workspace and communicate with the same Slack workspace. This document analyzes the consolidation into a single data source to eliminate redundant API calls and maintain data consistency.

---

## Current State

### ControlTower (This Project)

| Aspect | Details |
|--------|---------|
| **Purpose** | PMO observation, control, alerting system |
| **Database** | PostgreSQL (local/cloud) |
| **Sync Frequency** | Every 2 hours (configurable) |
| **Runtime** | Python + Streamlit UI |
| **Data Stored** | Projects, changelog, findings, sync_log |
| **Unique Features** | Rules engine, findings with severity, acknowledge workflow |

**Tables:**
- `projects` - Project metadata + raw_data JSONB
- `project_changelog` - Field-level change tracking
- `findings` - Rule violations with severity
- `sync_log` - Sync execution history

### BOTPMO (pulse-bot-mvp)

| Aspect | Details |
|--------|---------|
| **Purpose** | Slack bot for PM status updates collection |
| **Database** | DynamoDB (4 tables, pay-per-request) |
| **Sync Frequency** | Every 6 hours (cache refresh) |
| **Runtime** | Node.js Lambda + EventBridge |
| **Data Stored** | Projects cache, user profiles, updates, conversation state |
| **Unique Features** | Slack interaction, NLU agent, risk detection |

**Tables:**
- `pmo-bot-projects` - Global project cache (GSI: ResponsableIndex, PmoIdIndex)
- `pmo-bot-users` - Slack user profiles
- `pmo-bot-updates` - Project status updates from PMs
- `pmo-bot-conversations` - Conversation state (TTL: 1 hour)

---

## Data Model Comparison

### Projects Data

| Field | ControlTower | BOTPMO | Notes |
|-------|--------------|--------|-------|
| Project GID | `gid` (PK) | `pk: PROJECT#{gid}` | Same source |
| Name | `name` | `name` | Same |
| Owner | `owner_gid`, `owner_name` | `responsable`, `responsableKey` | Different naming |
| Due Date | `due_date` | `dueOn`, `dueAt` | Same source |
| Status | `status` (color) | `status` (text) | Different format |
| PMO ID | In `raw_data` custom_fields | `pmoId` (indexed) | BOTPMO has GSI |
| Progress | `calculated_progress` | `progressPercent` | Different calculation |
| Tasks | `total_tasks`, `completed_tasks` | `totalTasks`, `pendingTasks` | Same source |
| Last Activity | `last_activity_at` | `lastUpdateAt` | Different meaning |
| Raw Data | `raw_data` JSONB | Not stored | Only ControlTower |

### Unique to Each System

**ControlTower Only:**
- `project_changelog` - Field-level change history
- `findings` - Rule violations
- `sync_log` - Execution tracking
- Business Vertical filtering
- Schedule risk calculations

**BOTPMO Only:**
- `pmo-bot-users` - Slack user profiles
- `pmo-bot-updates` - PM-submitted status updates
- `pmo-bot-conversations` - Conversation state
- ResponsableIndex GSI for "mis proyectos"
- PmoIdIndex GSI for "PMO-XXX" lookup

---

## Consolidation Options

### Option 1: DynamoDB as Single Source (Recommended)

**Architecture:**
```
                    ┌─────────────────────────────────────┐
                    │           Asana API                  │
                    └─────────────────┬───────────────────┘
                                      │
                                      ▼
                    ┌─────────────────────────────────────┐
                    │   Unified Sync Lambda (Node.js)     │
                    │   - Every 2-4 hours                 │
                    │   - Fetches all project data        │
                    │   - Calculates metrics              │
                    └─────────────────┬───────────────────┘
                                      │
                                      ▼
                    ┌─────────────────────────────────────┐
                    │         DynamoDB (Shared)           │
                    ├─────────────────────────────────────┤
                    │  pmo-projects      (project cache)  │
                    │  pmo-changelog     (field changes)  │
                    │  pmo-findings      (rule violations)│
                    │  pmo-users         (Slack profiles) │
                    │  pmo-updates       (PM updates)     │
                    │  pmo-conversations (bot state)      │
                    │  pmo-sync-log      (sync history)   │
                    └───────────┬───────────────┬─────────┘
                                │               │
                    ┌───────────▼───────┐   ┌───▼───────────────┐
                    │   ControlTower    │   │     BOTPMO        │
                    │   (Streamlit)     │   │   (Slack Bot)     │
                    │   - Dashboard     │   │   - DM updates    │
                    │   - Rules engine  │   │   - NLU agent     │
                    │   - Findings UI   │   │   - Risk alerts   │
                    └───────────────────┘   └───────────────────┘
```

**Pros:**
- Single sync process, single API quota usage
- Serverless, pay-per-request (cost efficient)
- GSIs enable fast lookups (ResponsableIndex, PmoIdIndex)
- Already battle-tested with ~1,400 projects
- No server to maintain (vs PostgreSQL)

**Cons:**
- ControlTower needs significant refactor (Python → DynamoDB)
- DynamoDB query patterns less flexible than SQL
- No JOINs (denormalization required)
- Learning curve for DynamoDB modeling

**Effort Estimate:** High (2-3 weeks)

---

### Option 2: PostgreSQL as Single Source

**Architecture:**
```
                    ┌─────────────────────────────────────┐
                    │           Asana API                  │
                    └─────────────────┬───────────────────┘
                                      │
                                      ▼
                    ┌─────────────────────────────────────┐
                    │   Unified Sync (Python/Lambda)      │
                    └─────────────────┬───────────────────┘
                                      │
                                      ▼
                    ┌─────────────────────────────────────┐
                    │      PostgreSQL (RDS/Aurora)        │
                    └───────────┬───────────────┬─────────┘
                                │               │
                    ┌───────────▼───────┐   ┌───▼───────────────┐
                    │   ControlTower    │   │     BOTPMO        │
                    │   (no changes)    │   │   (refactor)      │
                    └───────────────────┘   └───────────────────┘
```

**Pros:**
- ControlTower requires minimal changes
- SQL flexibility (JOINs, complex queries)
- Familiar tooling (psql, pgAdmin)

**Cons:**
- BOTPMO Lambdas need refactor to use PostgreSQL
- Need managed PostgreSQL (RDS/Aurora) for Lambda access
- Higher base cost (always-on vs pay-per-request)
- VPC configuration complexity for Lambda

**Effort Estimate:** Medium-High (1-2 weeks)

---

### Option 3: Hybrid - DynamoDB + PostgreSQL Views

**Architecture:**
```
                    ┌─────────────────────────────────────┐
                    │   Unified Sync Lambda (Node.js)     │
                    └─────────────────┬───────────────────┘
                                      │
                    ┌─────────────────▼───────────────────┐
                    │         DynamoDB (Primary)          │
                    │   - Projects, Users, Updates        │
                    └─────────────────┬───────────────────┘
                                      │
                          DynamoDB Streams
                                      │
                    ┌─────────────────▼───────────────────┐
                    │   Sync Lambda (DynamoDB → PG)       │
                    └─────────────────┬───────────────────┘
                                      │
                    ┌─────────────────▼───────────────────┐
                    │      PostgreSQL (Secondary)         │
                    │   - Changelog, Findings, Reports    │
                    └─────────────────────────────────────┘
```

**Pros:**
- Best of both worlds
- BOTPMO unchanged
- ControlTower can use SQL for complex queries

**Cons:**
- Two databases to maintain
- Data sync complexity (DynamoDB Streams)
- Eventual consistency issues
- Higher operational overhead

**Effort Estimate:** High (3-4 weeks)

---

## Recommended Approach: Option 1 (DynamoDB)

### Rationale

1. **BOTPMO already works** - Production-tested with 1,400 projects
2. **Serverless alignment** - Both systems can be fully serverless
3. **Cost efficiency** - Pay-per-request vs always-on PostgreSQL
4. **Single sync** - One Lambda, one Asana API quota
5. **Existing GSIs** - ResponsableIndex and PmoIdIndex already useful

### Migration Strategy

#### Phase 1: Extend BOTPMO DynamoDB Schema

Add tables/attributes for ControlTower needs:

```
pmo-projects (existing, enhanced)
  pk: PROJECT#{gid}
  sk: META
  + raw_data: Map (full Asana response)
  + business_vertical: String
  + last_synced_at: String
  + sync_id: String

pmo-changelog (new)
  pk: PROJECT#{gid}
  sk: CHANGE#{timestamp}
  field_name: String
  old_value: String
  new_value: String
  sync_id: String

pmo-findings (new)
  pk: PROJECT#{gid}
  sk: FINDING#{rule_id}
  severity: String
  status: String (open|acknowledged|resolved)
  details: Map
  created_at: String
  acknowledged_at: String
  acknowledged_by: String
  slack_sent: Boolean
  GSI: StatusIndex (status -> project_gid)

pmo-sync-log (new)
  pk: SYNC#{sync_id}
  sk: META
  started_at: String
  completed_at: String
  projects_synced: Number
  changes_detected: Number
  findings_created: Number
  status: String
  error_message: String
```

#### Phase 2: Enhance Sync Lambda

Modify `cache-refresh.js` to:
1. Store `raw_data` for each project
2. Compare with previous values → write to `pmo-changelog`
3. Calculate ControlTower metrics (progress, task counts)
4. Extract Business Vertical for filtering

#### Phase 3: Create Rules Lambda

New Lambda function:
- Triggered after sync completes (EventBridge or Step Functions)
- Reads projects from DynamoDB
- Evaluates ControlTower rules
- Writes findings to `pmo-findings`
- Sends Slack alerts for new/changed findings

#### Phase 4: Migrate ControlTower UI

Modify Streamlit app to:
1. Read from DynamoDB instead of PostgreSQL
2. Use `boto3` DynamoDB client
3. Adapt queries to DynamoDB patterns
4. Maintain same UI/UX

#### Phase 5: Deprecate PostgreSQL

1. Run parallel for 2 weeks (validation)
2. Disable PostgreSQL sync
3. Archive/delete PostgreSQL database

---

## DynamoDB Access Patterns (Critical)

### ControlTower Needs

| Access Pattern | Current SQL | DynamoDB Solution |
|----------------|-------------|-------------------|
| List all projects | `SELECT * FROM projects` | Scan with filter (or GSI) |
| Filter by Business Vertical | `WHERE business_vertical = 'PS'` | GSI: `BusinessVerticalIndex` |
| Get project by GID | `WHERE gid = ?` | GetItem pk=PROJECT#{gid} |
| Get changelog for project | `WHERE project_gid = ?` | Query pk=PROJECT#{gid}, sk begins_with CHANGE# |
| List open findings | `WHERE status = 'open'` | GSI: `StatusIndex` |
| Findings by severity | `WHERE severity = 'high'` | GSI: `SeverityIndex` or filter |
| Recent sync logs | `ORDER BY started_at DESC LIMIT 10` | Query with sk sorting |

### New GSIs Required

```yaml
pmo-projects:
  - BusinessVerticalIndex:
      pk: businessVertical
      sk: pmoId

pmo-findings:
  - StatusIndex:
      pk: status
      sk: created_at
  - SeverityIndex:
      pk: severity
      sk: created_at
```

---

## Risks & Mitigations

### Risk 1: DynamoDB Query Limitations
**Impact:** High
**Description:** Complex queries (JOINs, aggregations) not possible in DynamoDB
**Mitigation:**
- Pre-calculate aggregations during sync
- Use GSIs strategically
- For reports, consider periodic export to S3 + Athena

### Risk 2: Data Migration Errors
**Impact:** High
**Description:** Data loss or corruption during migration
**Mitigation:**
- Run parallel systems during transition
- Implement data validation checks
- Keep PostgreSQL backup for 30 days post-migration

### Risk 3: Increased Complexity in Sync Lambda
**Impact:** Medium
**Description:** Single Lambda doing too much (sync + changelog + metrics)
**Mitigation:**
- Use Step Functions to orchestrate
- Split into focused Lambdas (sync → changelog → rules)

### Risk 4: Cost Increase with More Tables/GSIs
**Impact:** Low
**Description:** More GSIs = more write capacity units
**Mitigation:**
- DynamoDB on-demand pricing is still cheaper than RDS
- Monitor with CloudWatch
- Optimize GSI projections (KEYS_ONLY where possible)

### Risk 5: Learning Curve
**Impact:** Medium
**Description:** Team unfamiliar with DynamoDB patterns
**Mitigation:**
- Document access patterns thoroughly
- Create helper functions for common queries
- Pair programming during migration

---

## Decisions (Confirmed)

| Question | Decision |
|----------|----------|
| **Sync Frequency** | Full sync: daily (configurable). Partial sync: every 30 minutes (configurable) |
| **Rules Execution** | Run after each sync (both full and partial) |
| **Changelog Retention** | 365 days, configurable. Use DynamoDB TTL |
| **Findings Retention** | 365 days, configurable. Use DynamoDB TTL |
| **Historical Migration** | No migration from PostgreSQL. Recreate from scratch |
| **UI Deployment** | Local Streamlit for now. Cloud deployment in future |
| **Slack Channels** | Same channel for now. Separate channels in future (configurable) |
| **ControlTower Access** | Read AND Write to DynamoDB |
| **Transition Period** | None. Direct cutover once Streamlit uses DynamoDB |

---

## Implementation Prompts

Use these prompts sequentially to implement the consolidation.

### Prompt 1: Extend DynamoDB Schema

```
Context: BOTPMO project (pulse-bot-mvp), SAM template at infrastructure/template.yaml

Task: Extend the DynamoDB schema to support ControlTower features:

1. Add attributes to pmo-bot-projects table:
   - raw_data (Map) - full Asana response
   - business_vertical (String)
   - last_synced_at (String)
   - sync_id (String)

2. Create new table: pmo-bot-changelog
   - pk: String (PROJECT#{gid})
   - sk: String (CHANGE#{timestamp})
   - Attributes: field_name, old_value, new_value, sync_id, detected_at
   - Billing: PAY_PER_REQUEST

3. Create new table: pmo-bot-findings
   - pk: String (PROJECT#{gid})
   - sk: String (FINDING#{rule_id})
   - Attributes: severity, status, details (Map), created_at, acknowledged_at,
     acknowledged_by, ack_comment, slack_sent
   - GSI: StatusIndex (pk=status, sk=created_at)
   - Billing: PAY_PER_REQUEST

4. Create new table: pmo-bot-sync-log
   - pk: String (SYNC#{sync_id})
   - sk: String (META)
   - Attributes: started_at, completed_at, projects_synced, changes_detected,
     findings_created, status, error_message
   - Billing: PAY_PER_REQUEST

5. Add GSI to pmo-bot-projects:
   - BusinessVerticalIndex (pk=businessVertical, sk=pmoId)

Update the SAM template with these changes. Use consistent naming with
existing tables (pmo-bot-* prefix, Environment suffix).
```

### Prompt 2: Create DynamoDB Service Functions

```
Context: BOTPMO project, src/services/dynamo.js

Task: Add functions for the new tables:

1. Changelog functions:
   - saveChangelogEntry(projectGid, fieldName, oldValue, newValue, syncId)
   - getChangelogForProject(projectGid, limit=50)

2. Findings functions:
   - upsertFinding(projectGid, ruleId, severity, details)
   - getFindingsForProject(projectGid)
   - getOpenFindings() - query StatusIndex where status='open'
   - acknowledgeFinding(projectGid, ruleId, userId, comment)
   - resolveFinding(projectGid, ruleId)

3. Sync log functions:
   - createSyncLog(syncId) - creates with status='running'
   - completeSyncLog(syncId, stats) - updates with completion data
   - failSyncLog(syncId, errorMessage)
   - getRecentSyncLogs(limit=10)

4. Update upsertProjectCache to:
   - Accept raw_data parameter
   - Accept business_vertical parameter
   - Store sync_id and last_synced_at

Follow existing patterns in dynamo.js. Use DocumentClient.
```

### Prompt 3: Enhance Sync Lambda with Changelog

```
Context: BOTPMO project, src/handlers/cache-refresh.js

Task: Modify the cache refresh handler to:

1. Generate a sync_id (UUID) at start
2. Create sync_log entry with status='running'

3. For each project being synced:
   a. Fetch existing project from DynamoDB (if exists)
   b. Compare critical fields with new data:
      - status, responsable, dueOn, progressPercent, totalTasks, pendingTasks
   c. For each changed field, call saveChangelogEntry()
   d. Store raw Asana response in raw_data field
   e. Extract and store business_vertical from custom_fields

4. Track statistics:
   - projects_synced (count)
   - changes_detected (count of changelog entries)

5. At end, call completeSyncLog() with stats

6. On error, call failSyncLog() with error message

Keep existing functionality (parallel batching, delete completed projects).
Add appropriate logging for debugging.
```

### Prompt 4: Create Rules Engine Lambda

```
Context: BOTPMO project, create new handler src/handlers/rules-engine.js

Task: Create a Lambda handler that evaluates ControlTower rules:

1. Handler: exports.handler(event)
   - event.sync_id: The sync ID to process (from cache-refresh)

2. Load all projects from pmo-bot-projects where businessVertical is set

3. Implement rules (from ControlTower):

   Rule: no_status_update
   - Condition: lastUpdateAt is null OR > 7 days ago
   - Severity: medium

   Rule: no_activity
   - Condition: No task changes in last 7 days (compare with changelog)
   - Severity: medium

   Rule: schedule_risk
   - Conditions:
     - days_remaining <= 7 AND progress < 80% → severity: high
     - days_remaining <= 14 AND progress < 60% → severity: medium
     - days_remaining <= 30 AND progress < 40% → severity: low

   Rule: amount_of_tasks
   - Condition: totalTasks <= 3
   - Severity: medium

4. For each rule violation:
   - Call upsertFinding()
   - If finding is new or severity changed, mark for Slack alert

5. Update sync_log with findings_created count

6. Return summary of findings created/updated

Add to SAM template as new Lambda function, triggered by EventBridge
or invoked after cache-refresh completes.
```

### Prompt 5: Create Slack Alerts for Findings

```
Context: BOTPMO project, src/services/slack.js

Task: Add function to send ControlTower-style finding alerts:

1. sendFindingAlert(finding, project)
   - Format similar to existing sendAlertToPMO but for findings
   - Include: project name, rule violated, severity, details
   - Use emoji: high=🔴, medium=🟡, low=🟢
   - Include link to project in Asana

2. sendNewFindingsToSlack()
   - Query open findings where slack_sent != true
   - For each, call sendFindingAlert()
   - Update finding with slack_sent = true

3. Add to rules-engine.js:
   - After creating findings, call sendNewFindingsToSlack()

Use existing Slack channel (SlackChannelPMO) or add new parameter
for findings channel if different.
```

### Prompt 6: Create ControlTower DynamoDB Adapter (Python)

```
Context: ControlTower project, create src/controltower/db/dynamodb.py

Task: Create a DynamoDB adapter that mirrors the PostgreSQL interface:

1. Initialize boto3 DynamoDB resource
   - Use environment variables for region and table prefix
   - Table names: {prefix}-projects, {prefix}-changelog, etc.

2. Implement functions:

   Projects:
   - get_all_projects() -> List[dict]
   - get_project(gid: str) -> Optional[dict]
   - get_projects_by_business_vertical(vertical: str) -> List[dict]

   Changelog:
   - get_changelog_for_project(gid: str, limit: int = 50) -> List[dict]

   Findings:
   - get_all_findings(status: Optional[str] = None) -> List[dict]
   - get_findings_for_project(gid: str) -> List[dict]
   - acknowledge_finding(gid: str, rule_id: str, user: str, comment: str)
   - resolve_finding(gid: str, rule_id: str)

   Sync Log:
   - get_recent_sync_logs(limit: int = 10) -> List[dict]

3. Handle DynamoDB-specific patterns:
   - Parse pk/sk formats (PROJECT#123 -> 123)
   - Convert DynamoDB types to Python types
   - Handle pagination for scans

Use boto3 resource API (not client) for cleaner code.
Add proper error handling and logging.
```

### Prompt 7: Migrate Streamlit UI to DynamoDB

```
Context: ControlTower project, src/controltower/ui/app.py

Task: Modify the Streamlit app to use DynamoDB instead of PostgreSQL:

1. Replace imports:
   - Remove: from controltower.db.connection import get_engine
   - Add: from controltower.db.dynamodb import DynamoDBAdapter

2. Initialize DynamoDB adapter instead of SQLAlchemy engine

3. Update all data fetching to use new adapter:
   - Projects list: adapter.get_all_projects()
   - Project details: adapter.get_project(gid)
   - Changelog: adapter.get_changelog_for_project(gid)
   - Findings: adapter.get_all_findings(status='open')

4. Update acknowledge functionality:
   - Call adapter.acknowledge_finding()

5. Maintain existing UI/UX:
   - Same filters (status, PMO ID, responsable)
   - Same columns and formatting
   - Same Excel export (adapt data structure)

6. Add configuration:
   - Environment variable: USE_DYNAMODB=true
   - Fallback to PostgreSQL if false (for transition period)

Test thoroughly with real data before removing PostgreSQL code.
```

---

## Timeline Estimate

| Phase | Duration | Dependencies |
|-------|----------|--------------|
| 1. Extend DynamoDB Schema | 2 days | None |
| 2. DynamoDB Service Functions | 2 days | Phase 1 |
| 3. Enhance Sync with Changelog | 3 days | Phase 2 |
| 4. Rules Engine Lambda | 3 days | Phase 3 |
| 5. Slack Alerts for Findings | 1 day | Phase 4 |
| 6. ControlTower DynamoDB Adapter | 3 days | Phase 2 |
| 7. Migrate Streamlit UI | 3 days | Phase 6 |
| 8. Testing & Validation | 3 days | All phases |
| 9. Parallel Run & Cutover | 5 days | Phase 8 |

**Total: ~25 days (5 weeks)**

---

## Success Criteria

1. **Single Sync:** Only one system calls Asana API
2. **Data Consistency:** Both apps show same project data
3. **Feature Parity:** All ControlTower features work with DynamoDB
4. **Performance:** Dashboard loads in < 3 seconds
5. **Cost:** DynamoDB costs < PostgreSQL hosting costs
6. **Reliability:** Zero data loss during migration
