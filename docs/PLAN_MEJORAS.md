# PLAN_MEJORAS (Táctico)

## Top 3 mejoras con mayor ganancia inmediata

### 1) Inbox por Responsable + Cierre de loop (más importante)
**Objetivo:** pasar de “ver hallazgos” a “resolverlos”.  
**Beneficio:** mejora rápida de higiene PMO y accountability.

### 2) Severidad dinámica y priorización
**Objetivo:** reducir ruido y enfocar en lo crítico.  
**Beneficio:** menos spam, más foco de PMO.

### 3) Reglas de higiene de datos (campos obligatorios)
**Objetivo:** asegurar calidad mínima (PMO‑ID, Cliente, Responsable, Presupuesto).  
**Beneficio:** mejora la confianza y precisión de reportes.

---

## Paso a paso (Mejora 1: Inbox por Responsable + Cierre de loop)

### Paso 1 — Definir estados de hallazgo
Agregar estados:
- `open`
- `acknowledged`
- `resolved`
- `snoozed`
- `false_positive`

### Paso 2 — Modelo de acción
Crear tabla `findings_actions`:
- `finding_id`
- `action` (ack/resolve/snooze/false_positive)
- `comment`
- `user`
- `created_at`

### Paso 3 — UI Inbox Responsable
En Streamlit:
- Selector Responsable → lista de hallazgos (solo ese responsable).
- Botones de acción por fila.
- Comentario obligatorio en acciones críticas.

### Paso 4 — SLA y seguimiento
Mostrar KPIs por responsable:
- % hallazgos abiertos
- % resueltos en 7 días
- antigüedad promedio

### Paso 5 — Notificación (opcional)
Enviar recordatorio si hay hallazgos abiertos > 7 días.

---

## Criterio de éxito
- 80% de hallazgos con acción en 7 días.
- Disminución de proyectos sin updates en 2 semanas.

---

## KPI 1 — % de cumplimiento de updates por semana (recomendado)

**Objetivo:** medir cumplimiento de reporting considerando duración real de los proyectos.  
**Unidad de control:** semana activa por proyecto.

### Definición
Un proyecto **cumple** una semana si tuvo **al menos un status update** dentro de esa semana calendario (Lun–Dom).  
Se suman todas las semanas activas de todos los proyectos del responsable.

### Fórmula
```
KPI = (Semanas cumplidas / Semanas totales) * 100
```

### Ejemplo
- PM A: 1 proyecto de 8 semanas, incumple 1 semana  
  → 7/8 = 87.5%
- PM B: 1 proyecto de 4 semanas, incumple 4 semanas  
  → 0/4 = 0%

### Nota técnica
Puede calcularse con:
- snapshots semanales (recomendado), o
- cálculo “on the fly” usando `status_updates.created_at`.
