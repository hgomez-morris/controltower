# Mejoras Consolidadas (ChatGPT + Gemini)

**Fuente:** Comparación entre `MejorasAFuturo.md` (ChatGPT) y `MejorasAFuturo_Gemini.md` (Gemini)  
**Objetivo:** Unificar lo mejor de ambos enfoques y priorizar impacto PMO.

---

## 1) Resumen Ejecutivo
La Control Tower ya consolidó datos, reglas y hallazgos. La oportunidad es pasar de **visor operativo** a **motor de gobernanza activo**:
- Detectar riesgos reales (consistencia, velocidad, salud)
- Priorizar acciones (severidad/impacto)
- Cerrar el loop (acción → resolución)

---

## 2) Pilares Estratégicos Consolidados

### A) Gobernanza y Ciclo de Vida
- Estado PMO interno (Inicio → Ejecución → Riesgo → Pausa → Cerrado).
- Reglas de transición + SLA de comunicación.
- Historial y auditabilidad de cambios.

### B) Inteligencia y Consistencia
- **Semáforo declarado vs semáforo calculado** (detección de “sandía”).
- Tendencias de avance (velocidad semanal, burn‑up).
- Detección de “sliding dates” (movimientos repetidos de fecha fin).

### C) Prioridad Real (Severidad)
- Matriz de severidad basada en:
  - impacto (cliente, presupuesto),
  - urgencia (fecha de cierre),
  - estado actual + reglas simultáneas.
- Escalada cuando 2+ reglas activas coinciden.

### D) Operación y Automatización
- Mensajes consolidados por responsable (ya implementado).
- Digest semanal PMO (top críticos).
- Alertas solo ante cambios (evitar spam).

---

## 3) Mejoras Funcionales (Detalle)

### 3.1 Reglas
- `no_status_update`: umbral por tipo de proyecto.
- `no_tasks_activity_last_7_days`: ignorar “sin tareas” para no duplicar motivo.
- `amount_of_tasks`: parametrizar por tipo/POC.
- `schedule_risk`: usar fecha planificada si existe.

### 3.2 Calidad de Datos
- Validación de campos obligatorios:
  - PMO ID, Cliente, Responsable, Presupuesto.
- Reporte de “higiene” de datos por sponsor/responsable.

### 3.3 UI / Experiencia
- **Vista ejecutiva** (KPIs + top críticos).
- **Focus Mode PM** (solo mis proyectos + hallazgos).
- “Inbox de Responsable” con acciones rápidas.
- Timeline macro (Gantt de alto nivel).

### 3.4 Insights adicionales
- KPI de comunicación: % proyectos con update < 7 días.
- Detección de “riesgo de planificación” por changelog de fechas.
- Comparación de avance real vs esperado (por semanas).

---

## 4) Integraciones

### SlackOps
- Mensajes directos con resumen por responsable.
- Botón “Resolver / Acknowledge” en Slack (v2).

### Asistencia con IA (v2/v3)
- Redacción automática de borradores de status.
- Análisis de sentimiento del texto del update.

---

## 5) Roadmap Priorizado (Impacto x Esfuerzo)

### Corto plazo (Q1)
- Reglas de higiene + severidad real.
- Vista ejecutiva simple (KPIs + top críticos).
- “Inbox Responsable” (mensajes consolidados + acciones).

### Mediano plazo (Q2)
- Detección de “sliding dates”.
- Tendencias de avance (burn‑up).
- Digest semanal automatizado.

### Largo plazo (Q3)
- Análisis de sentimiento y consistencia (IA).
- Predicción de fecha de término.

---

## 6) Notas Técnicas
- El esquema actual soporta extensiones.
- Si se agrega trend analysis, conviene una tabla de `daily_snapshots`.
- Para IA, se recomienda pipeline asíncrono para no bloquear el sync.

