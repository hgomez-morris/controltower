# agents.md — Guía operativa para implementar PMO Control Tower (MVP)

Este repositorio está diseñado para ser ejecutado por un agente/IA asistido por humanos.
El objetivo es **implementar el MVP** del PMO Control Tower con:
- Sync read-only desde Asana cada 2 horas (o manual durante desarrollo)
- Cálculo de 3 reglas (no_status_update, no_activity, schedule_risk)
- Persistencia en PostgreSQL
- Alertas vía Slack Incoming Webhook
- UI Streamlit para visualización y filtros

## 0) Principio de seguridad (NO NEGOCIABLE)
**Este MVP NO debe modificar Asana.**
- Prohibido: crear/editar proyectos, tareas, status updates, campos custom, comentarios.
- Permitido: **solo lectura** por API (GET).
- Excepción: cualquier acción que modifique Asana debe quedar explícitamente fuera de alcance (v2+)
  y requerirá aprobación humana por escrito.

### Checklist de cumplimiento
Antes de mergear cambios:
- [ ] No existen llamadas POST/PUT/PATCH/DELETE hacia la API de Asana.
- [ ] El token de Asana tiene permisos mínimos (lectura).
- [ ] Toda funcionalidad que “parezca” modificar Asana está deshabilitada por feature flag
      y documentada como v2+.

## 1) Definición del MVP (2 semanas)
Entregables MVP:
1) Data Collector: sincroniza proyectos + tareas + status updates
2) Diff / changelog **acotado a campos críticos** (no audit-all en MVP)
3) Rules Engine: genera findings
4) Slack Notifier: envía alertas sólo para *nuevos* findings (evitar spam)
5) Streamlit UI: dashboard + lista de proyectos + lista de hallazgos + detalle

## 2) Campos críticos para changelog (MVP)
El changelog MVP debe auditar solo:
- due_date
- owner (gid + name)
- status (on_track / at_risk / off_track si existe)
- last_status_update_at + last_status_update_by
- total_tasks, completed_tasks, calculated_progress
- last_activity_at (derivado)

Todo lo demás (raw dump JSON) puede guardarse en `projects.raw_data` sin diff detallado.

## 3) Reglas MVP (determinísticas)
### Regla: no_status_update
- Condición: `days_since_last_status_update > 7`
- Severidad base: medium

### Regla: no_activity
- Condición: `tasks_created_last_7d == 0 AND tasks_completed_last_7d == 0`
- Severidad base: medium

### Regla: schedule_risk
Comparar `days_remaining` vs `calculated_progress`:
- days_remaining <= 7  AND progress < 80 => high
- days_remaining <= 14 AND progress < 60 => medium
- days_remaining <= 30 AND progress < 40 => low

## 4) Visibilidad / “Escalamiento” (MVP)
En MVP la visibilidad debe ser **solo JP → PMO**.
- El sistema notifica:
  - JP (DM o mención según configuración)
  - Canal PMO (p. ej. #pmo-status)
- No existe CTO/CEO en MVP.
- No hay auto-escalamiento por días en MVP.

## 5) Acciones permitidas (MVP)
- Slack: enviar alerta de nuevo hallazgo
- DB: guardar hallazgos + historial
- UI: permitir **Acknowledge** con comentario obligatorio (no “resolver/borrar”)

### Acknowledge (UI)
Cuando un usuario PMO marca un finding como Acknowledged:
- Debe dejar comentario obligatorio
- Debe registrar `acknowledged_at`, `acknowledged_by`, `ack_comment`
- El finding NO se elimina. Permanece auditable.

## 6) Reglas de notificación (anti-spam)
- Alertar solo cuando:
  - se crea un finding nuevo, o
  - cambia severidad (ej. medium -> high)
- No reenviar cada sync el mismo finding.
- (Opcional) Un resumen diario puede agregarse en v2+.

## 7) Configuración (config.yaml)
Toda regla/umbral/canales se define en YAML.
No hardcodear:
- umbrales de días
- thresholds schedule_risk
- canal Slack / mención JP
- workspace_gid

## 8) Entornos
### Desarrollo local
- PostgreSQL local vía docker-compose
- Streamlit local
- Sync manual (comando) y luego cron simulado

### Producción MVP
- Puede correr en una EC2 pequeña o servidor interno
- Cron del sistema ejecuta sync cada 2h
- Streamlit sirve UI interna

## 9) Definition of Done (MVP)
- [ ] Sync funciona para 100–150 proyectos activos sin fallar
- [ ] Se registran projects + métricas de tareas
- [ ] Se generan findings consistentes para 3 reglas
- [ ] Slack recibe alertas (solo nuevos)
- [ ] Streamlit permite filtrar por regla/severidad/JP/proyecto
- [ ] Acknowledge guarda comentario y auditoría

## 10) Estándar de ingeniería
- Logs estructurados (nivel INFO por defecto, DEBUG opcional)
- Manejo de rate limits de Asana (retry con backoff)
- No exponer secretos en logs
- Tests unitarios básicos para reglas y cálculo de métricas

## 11) Ruta local sugerida (Windows)
Durante el desarrollo local, clonar en:
`C:\MorrisFiles\Proyectos\ControlTower`
