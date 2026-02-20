# Data Dictionary — ControlTower

Este documento describe **qué hace cada tabla**, sus **campos**, y en el caso de columnas `raw` (JSONB) incluye **qué contienen** y un **ejemplo**. Está pensado para que una IA entienda el contexto completo de datos.

> Fuente principal: `src/controltower/db/schema.sql` y flujo de sync (`src/controltower/sync/sync_runner.py`, `scripts/run_clockify_sync.py`).

---

## Tablas Asana (schema público)

### `projects`
**Qué es:** Tabla principal de proyectos sincronizados desde Asana (solo lectura).  
**Clave:** `gid`.

Campos:
- `gid` (PK) — ID del proyecto en Asana.
- `name` — Nombre del proyecto.
- `owner_gid`, `owner_name` — Responsable principal en Asana.
- `due_date` — Fecha final (Asana).
- `status` — Estado derivado de `current_status.color` (ej: `green`, `yellow`, `red`, `blue`).
- `calculated_progress` — % de tareas completas.
- `last_status_update_at`, `last_status_update_by` — último status update.
- `last_activity_at` — última actividad en tareas.
- `total_tasks`, `completed_tasks` — métricas de tareas.
- `tasks_created_last_7d`, `tasks_completed_last_7d`, `tasks_modified_last_7d` — actividad reciente.
- `start_date` — fecha de inicio (custom field “Fecha Inicio del Proyecto”).
- `planned_hours_total` — horas planificadas (custom field “Horas planificadas”).
- `effective_hours_total` — horas efectivas (custom field “Horas efectivas”).
- `pmo_id` — Custom field “PMO ID”.
- `sponsor` — Custom field “Sponsor”.
- `responsable_proyecto` — Custom field “Responsable Proyecto”.
- `business_vertical` — Custom field “Business Vertical”.
- `fase_proyecto` — Custom field “Fase del proyecto”.
- `en_plan_facturacion` — Custom field “En plan de facturación” (sí/no).
- `completed_flag` — flag completado (Asana).
- `raw_data` (JSONB) — payload completo de proyecto Asana.
- `synced_at` — timestamp del sync.
- `created_at` — timestamp de inserción local.

**`raw_data` contiene:**  
Objeto con `project` (payload Asana), `tasks` (lista mínima) y metadatos de sync.

Ejemplo (simplificado):
```json
{
  "project": {
    "gid": "120123456789",
    "name": "PMO-1234 Implementación",
    "owner": {"gid": "321", "name": "Maria Perez"},
    "due_date": "2026-03-31",
    "completed": false,
    "current_status": {"created_at": "2026-02-10T12:00:00Z", "color": "green"},
    "custom_fields": [
      {"name": "PMO ID", "display_value": "PMO-1234"},
      {"name": "Sponsor", "display_value": "Cliente X"},
      {"name": "Business Vertical", "enum_value": {"name": "Professional Services"}}
    ]
  },
  "tasks": [
    {"gid": "t1", "created_at": "2026-02-01T10:00:00Z", "completed": false}
  ],
  "sync": {"run_id": "abc-123", "timestamp": "2026-02-19T09:00:00Z"}
}
```

---

### `project_changelog`
**Qué es:** Cambios detectados en campos críticos entre sincronizaciones.

Campos:
- `id` (PK)
- `project_gid` (FK → `projects.gid`)
- `field_name` — campo cambiado (ej: `status`, `due_date`).
- `old_value`, `new_value` — valores anteriores/nuevos.
- `changed_at` — tiempo reportado por Asana (si aplica).
- `detected_at` — tiempo de detección local.
- `sync_id` — ID del sync.

---

### `findings`
**Qué es:** Hallazgos generados por reglas (MVP).

Campos:
- `id` (PK)
- `project_gid` (FK → `projects.gid`)
- `rule_id` — regla (`no_status_update`, `no_tasks_activity_last_7_days`, etc).
- `severity` — `low|medium|high`
- `status` — `open|acknowledged|resolved`
- `details` (JSONB) — datos específicos de la regla.
- `created_at`, `acknowledged_at`, `acknowledged_by`, `ack_comment`, `resolved_at`.

Ejemplo `details`:
```json
{"days_since_last_status_update": 12}
```

---

### `status_updates`
**Qué es:** Status updates de proyecto en Asana.

Campos:
- `gid` (PK) — ID del status update.
- `project_gid` (FK)
- `created_at`, `author_gid`, `author_name`
- `status_type` — tipo Asana
- `title`, `text`, `html_text`
- `raw_data` (JSONB) — payload completo Asana.
- `synced_at`

Ejemplo `raw_data` (simplificado):
```json
{
  "gid": "su_123",
  "created_at": "2026-02-10T12:00:00Z",
  "status_type": "on_track",
  "title": "Semana 6",
  "text": "Avance ok",
  "author": {"gid": "321", "name": "Maria Perez"}
}
```

---

### `status_update_comments`
**Qué es:** Comentarios asociados a status updates.

Campos:
- `id` (PK)
- `status_update_gid` (FK → `status_updates.gid`)
- `story_gid` — ID del comentario (Asana story)
- `created_at`, `author_gid`, `author_name`
- `text`, `html_text`
- `raw_data` (JSONB)
- `synced_at`

---

### `sync_log`
**Qué es:** Auditoría de sincronizaciones Asana.

Campos:
- `id` (PK)
- `sync_id` (UNIQUE)
- `started_at`, `completed_at`
- `projects_synced`, `changes_detected`, `findings_created`
- `status` (running/completed/failed)
- `error_message`

---

### `kpi_snapshots`
**Qué es:** Snapshot de KPIs calculados.

Campos:
- `id` (PK)
- `kpi_id` — ID lógico del KPI
- `scope_type` — `empresa|sponsor|jp`
- `scope_value` — valor del scope
- `as_of` — timestamp
- `total_projects`, `compliant_projects`, `kpi_value`

---

### `projects_history`
**Qué es:** Histórico de proyectos cerrados o fuera de scope.

Campos:
- `gid` (PK)
- `name`, `owner_gid`, `owner_name`
- `status`, `last_status_update_at`, `last_status_update_by`
- `pmo_id`, `cliente_nuevo`, `responsable_proyecto`, `sponsor`
- `aws_opp_id`, `id_comercial`
- `search_text` — texto preprocesado para búsqueda
- `raw_data` (JSONB) — snapshot original
- `snapshot_at`

---

### `payments`
**Qué es:** Registro de pagos asociados a PMO-ID (no vienen de Asana).

Campos:
- `id` (PK)
- `project_gid` — opcional (si se resolvió el proyecto)
- `pmo_id` — identificador del proyecto
- `status` — `estimado|efectuado`
- `payment_date` — fecha asociada
- `glosa` — notas
- `created_at`, `updated_at`

---

### `payment_estimate_history`
**Qué es:** Historial de cambios de fecha para pagos estimados.

Campos:
- `id` (PK)
- `payment_id` (FK → `payments.id`)
- `old_date`, `new_date`
- `changed_at`

---

## Tablas Clockify (schema `clockify`)

Estas tablas se generan y llenan por `scripts/run_clockify_sync.py`.

### `clockify.workspaces`
**Qué es:** Workspaces sincronizados.
- `workspace_id`, `source`, `updated_at`

### `clockify.clients`
**Qué es:** Clientes de Clockify.
- `client_id`, `workspace_id`, `name`, `updated_at`

### `clockify.projects`
**Qué es:** Proyectos de Clockify.
- `project_id`, `workspace_id`, `client_id`, `name`, `billable`, `is_public`, `color`,
  `archived`, `updated_at`, `hourly_rate`, `estimate`, `duration`, `budget_type`

### `clockify.people`
**Qué es:** Usuarios/personas.
- `person_id`, `workspace_id`, `name`, `updated_at`

### `clockify.tasks`
**Qué es:** Tareas de Clockify (si existen).
- `task_id`, `workspace_id`, `project_id`, `name`, `updated_at`

### `clockify.tags`
**Qué es:** Tags de time entries.
- `tag_id`, `workspace_id`, `name`, `updated_at`

### `clockify.time_entries`
**Qué es:** Time entries detallados.
Campos principales:
- `time_entry_id` (PK lógico)
- `workspace_id`, `project_id`, `task_id`, `user_id`
- `description`, `is_billable`
- `entry_date` (DATE)
- `start_time`, `end_time`
- `duration_seconds`, `hours`
- `updated_at`

### `clockify.time_entry_tags`
**Qué es:** Relación N:N entre time entries y tags.
- `time_entry_id`, `tag_id`

### `clockify.calendar_weeks`
**Qué es:** Tabla auxiliar para agrupación por semanas.
- `week_start`, `week_end`

### `clockify.sync_runs`
**Qué es:** Runs del sincronizador Clockify.
- `run_id`, `started_at`, `completed_at`
- `status`, `source_type`, `source_reference`, `workspace_id`
- métricas de run

### `clockify.sync_history`
**Qué es:** Últimos eventos de sync.
- `synced_at`, `status`, `source_reference`

### `clockify.v_person_weekly_hours` (vista)
**Qué es:** Vista usada en UI para horas por semana/persona.
- `person_name`, `week_start`, `total_hours`

---

## Notas de `raw_data`

Campos `raw_data` almacenan el payload completo de la fuente (Asana) para trazabilidad.  
No se usan directamente en UI salvo para extraer custom fields o enriquecer vistas.

Recomendación: si se agregan nuevos campos en Asana, **primero** revisar `raw_data` y luego decidir si se promueve a columna normalizada.

---

## Tablas ML (schema `ml`)

### `ml.project_id_map`
**Qué es:** mapa PMO-ID ↔ Asana ↔ Clockify.
- `pmo_id` (PK)
- `asana_project_gid`
- `clockify_project_id`
- `updated_at`

### `ml.weekly_fact`
**Qué es:** agregado semanal por proyecto (Clockify).
- `pmo_id`
- `week_start`
- `hours_week`
- `active_users_week`
- `updated_at`

### `ml.ml_project_labels`
**Qué es:** etiqueta supervisada (desviación final).
- `pmo_id`
- `final_deviation`
- `label`
- `computed_at`

### `ml.ml_project_features`
**Qué es:** features de ML por proyecto y ventana `k`.
- `pmo_id`
- `k`
- `ratio_burn`
- `slope_hours_week`
- `volatility_hours`
- `active_people_k`
- `people_growth`
- `hours_top1_share`
- `jp_active_projects_k`
- `jp_total_hours_k`
- `jp_utilization`
- `log_planned_hours`
- `computed_at`

### `ml.ml_project_scores`
**Qué es:** puntuaciones del modelo (probabilidad).
- `pmo_id`
- `k`
- `probability`
- `scoring_date`
- `model_version`
- `computed_at`

### `ml.ml_data_quality_issues`
**Qué es:** registro de problemas de calidad de datos.
- `id`
- `pmo_id`
- `issue_type`
- `details`
- `detected_at`
