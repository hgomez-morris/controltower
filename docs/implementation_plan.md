# Plan de implementación (MVP)

## Semana 1 — Data + Rules
**Día 1–2**
- Conexión read-only a Asana API (token)
- Modelo de datos mínimo en PostgreSQL
- Sync básico de proyectos + tareas + statuses

**Día 3**
- Diff en campos críticos
- Changelog funcionando (acotado)

**Día 4–5**
- Rules Engine con 3 reglas:
  - no_status_update
  - no_activity
  - schedule_risk
- Persistencia de findings

## Semana 2 — Acciones + UI
**Día 6–7**
- Integración Slack Webhook
- Anti-spam: solo nuevos hallazgos

**Día 8–9**
- Streamlit UI:
  - Dashboard
  - Lista hallazgos (filtros y detalle)
  - Acknowledge con comentario obligatorio

**Día 10**
- Testing (reglas, rate limiting, DB)
- Ajustes y preparación para deploy (cron + server)
