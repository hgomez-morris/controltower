# agents.md — Guía operativa PMO Control Tower (MVP+)

Este repositorio está diseñado para ser ejecutado por un agente/IA asistido por humanos.
El objetivo es **implementar el MVP** del PMO Control Tower con:
- Sync read-only desde Asana (manual o programado)
- Cálculo de reglas y hallazgos
- Persistencia en PostgreSQL
- Mensajería vía Slack (webhook + bot para DM)
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
1) Data Collector: sincroniza proyectos + tareas + status updates + comentarios
2) Diff / changelog acotado a campos críticos
3) Rules Engine: genera findings
4) Slack: mensajes manuales (consolidado por responsable)
5) Streamlit UI: dashboard + proyectos + findings + seguimiento + mensajes

## 2) Campos críticos para changelog (MVP)
El changelog MVP audita:
- due_date
- owner (gid + name)
- status (on_track / at_risk / off_track / on_hold)
- last_status_update_at + last_status_update_by
- total_tasks, completed_tasks, calculated_progress
- last_activity_at (derivado)

Todo lo demás puede guardarse en `projects.raw_data` sin diff detallado.

## 3) Reglas (MVP)
### Regla: no_status_update
- Condición: `days_since_last_status_update > 7`
- Severidad base: medium

### Regla: no_tasks_activity_last_7_days
- Condición: `tasks_created_last_7d == 0 AND tasks_completed_last_7d == 0`
- Severidad base: medium

### Regla: schedule_risk
Comparar `days_remaining` vs `calculated_progress`:
- days_remaining <= 7  AND progress < 80 => high
- days_remaining <= 14 AND progress < 60 => medium
- days_remaining <= 30 AND progress < 40 => low

### Regla: amount_of_tasks
- Condición: `total_tasks <= 3`
- Severidad base: medium

## 4) Alcance de datos
- Solo proyectos con **PMO ID**.
- Solo proyectos con **Business Vertical = Professional Services**.
- Proyectos **completados/terminados/cancelados**:
  - Se sincronizan **solo si** su cierre fue en los últimos 30 días.
  - **Se excluyen** de todas las grillas y gráficos.

## 5) Visibilidad / Escalamiento (MVP)
En MVP la visibilidad es **solo JP → PMO**.
- El sistema genera mensajes por responsable (manual).
- No hay auto-escalamiento por días en MVP.

## 6) Mensajería Slack
- Webhook para canal (mensajes manuales).
- Bot token (`SLACK_BOT_TOKEN`) para DM directo por email.
- Mensaje consolidado por responsable:
  - Un solo mensaje con PMO-ID, nombre y motivo.
  - Excluye `schedule_risk`.

## 7) Status updates y comentarios
Se almacenan en tablas:
- `status_updates`
- `status_update_comments`

Se muestran en la UI (modal en Proyectos + grillas de Seguimiento).

## 8) Configuración
Toda regla/umbral/canales se define en YAML.
No hardcodear:
- umbrales de días
- thresholds schedule_risk
- canal Slack / mención JP
- workspace_gid

Variables en `.env`:
- `ASANA_ACCESS_TOKEN`
- `ASANA_WORKSPACE_GID`
- `SLACK_WEBHOOK_URL`
- `SLACK_CHANNEL`
- `SLACK_BOT_TOKEN`
- `DB_*`

## 9) Entornos
### Desarrollo local
- PostgreSQL local vía docker-compose
- Streamlit local
- Sync manual y cron simulado

### Sync manual (Windows / Git Bash)
1) Cargar variables de entorno desde `.env` (incluye workaround para BOM):
   - `set -a; . <(sed -e '1s/^\xEF\xBB\xBF//' .env); set +a`
2) Definir `PYTHONPATH`:
   - `export PYTHONPATH="$(pwd)/src"`
3) Ejecutar sync:
   - `python scripts/run_sync.py`

### Sync paralelo (opcional)
- Script: `python scripts/run_sync_parallel.py`
- Workers (por defecto 4): `SYNC_WORKERS=8` para ajustar concurrencia.

### Carga histórica (una sola vez)
Inserta solo proyectos que NO están en `projects`.
- Ejecutar: `python scripts/load_projects_history.py`
- Si hay proyectos sin acceso (403), quedan en `logs/forbidden_projects.log`.

### Sync programado (Windows Task Scheduler, cada 4 horas)
Usar un `.bat` que cargue `.env`, setee `PYTHONPATH` y ejecute `scripts/run_sync.py`.
Luego, crear una tarea con el trigger “Daily” y “Repeat task every: 4 hours”.

### Producción MVP
- Puede correr en EC2/servidor interno
- Cron del sistema ejecuta sync cada 2h
- Streamlit sirve UI interna

### Búsqueda local
La página “Búsqueda” consulta primero `projects` (sync) y luego `projects_history`.
Si un `gid` existe en ambas, se muestra la versión de `projects`.

## 10) Definition of Done (MVP)
- [ ] Sync funciona para 100–150 proyectos activos sin fallar
- [ ] Se registran proyectos + métricas + updates + comentarios
- [ ] Se generan findings consistentes para reglas definidas
- [ ] Mensajes consolidados listos para envío (manual/Slack)
- [ ] Streamlit permite filtrar por regla/severidad/JP/proyecto
- [ ] Acknowledge guarda comentario y auditoría

## 11) Ruta local sugerida (Windows)
Durante el desarrollo local, clonar en:
`C:\MorrisFiles\Proyectos\ControlTower`

## 12) Estado del proyecto (pausa)
**Fecha de pausa:** 9 de febrero de 2026.

Mientras el proyecto esté en pausa:
- No ejecutar cron ni sync automático.
- No desplegar cambios a producción.

Para retomar:
- Verificar `.env` y credenciales vigentes.
- Levantar servicios locales (`docker-compose`, Streamlit) y ejecutar un sync manual.
- Revisar pendientes en `docs\PLAN_MEJORAS.md` y `docs\Mejoras_consolidadas_chatgpt.md`.
