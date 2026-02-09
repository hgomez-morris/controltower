# Mejoras a Futuro (Visión PMO)

## Objetivo general
Consolidar la Control Tower como herramienta de gobernanza y priorización PMO, con foco en:
- Visibilidad accionable
- Gestión por desempeño
- Cierre de loop (acción → resolución)

---

## 1) Mejoras de alto nivel (estratégicas)

### 1.1 Gobernanza del ciclo de vida
Crear un estado PMO interno (distinto al de Asana):
Inicio → En ejecución → En riesgo → En pausa → Cerrado  
con historial, reglas de transición y SLA de reporting.

### 1.2 KPIs PMO y SLA de reporting
KPIs por responsable y por sponsor:
- % proyectos con update < 7 días
- % proyectos sin tareas o sin actividad
- Semáforo PMO global y por área

### 1.3 Priorización por severidad real
Matriz de severidad combinando:
riesgo + cliente + presupuesto + fecha crítica.  
Proyectos con ≥2 reglas activas suben de severidad.

### 1.4 Cierre de loop
Agregar plan de acción por hallazgo:
fecha compromiso, dueño, estado y escalamiento automático.

---

## 2) Mejoras funcionales (detalle)

### 2.1 Reglas
- `no_tasks_activity_last_7_days`: excluir proyectos sin tareas (regla separada)
- `no_status_update`: umbrales distintos por tipo de proyecto
- `amount_of_tasks`: parametrizable según tipo/POC
- `schedule_risk`: usar fecha planificada (custom field) en vez de due_date

### 2.2 UX / Operación diaria
- Página “Responsables” tipo inbox (proyectos + hallazgos + mensaje)
- Vista “Proyectos críticos” (2+ reglas simultáneas)
- Filtros por cliente/sponsor en dashboard
- Vista ejecutiva simplificada (KPIs y semáforos)

### 2.3 Automatización
- Digest semanal PMO
- Alertas solo por cambios nuevos (no repetitivos)
- Resúmenes de top proyectos críticos

### 2.4 Calidad de datos
- Reglas para campos críticos vacíos (PMO ID, Cliente, Responsable)
- Historial de cambios y tendencias

### 2.5 Integración y exportación
- Export con filtros aplicados
- DM automático con override manual

---

## 3) Roadmap sugerido

### Semana 1–2 (MVP+)
- Ajuste severidades reales
- Vista por responsable
- Cierre de loop (ack + fecha compromiso)

### Semana 3–4
- KPIs por sponsor/responsable
- Digest automático
- Priorización por combinación de reglas

