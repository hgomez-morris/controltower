# Revisión Técnica — Performance, Usabilidad, Mantenibilidad, Buenas Prácticas

Fecha: 2026-02-18

Este documento recoge mejoras detectadas tras la refactorización multipágina y la integración de Clockify.
Se sugiere abordarlas **de una en una**, priorizando por impacto y riesgo.

---

## P0 — Riesgos / bugs potenciales

1) **Menú custom: radios en grupos con estado cruzado**
   - Estado actual: se usan 3 radios independientes, con “limpieza” de los otros dos.
   - Riesgo: usuarios confusos si el estado queda inconsistente tras recarga o cambios de estado.
   - Recomendación: usar **un solo radio** con secciones visuales (o un select) para navegación única.

2) **Timezone consistente en sync labels**
   - Asana usa `ZoneInfo("America/Santiago")`. Clockify usa conversión manual.
   - Recomendación: centralizar `format_datetime_chile()` en `ui/lib/common.py` y reutilizar en ambos lados.

3) **Clockify sync dentro de `run_sync.py`**
   - Actualmente `run_sync.py` ejecuta Clockify post Asana.
   - Riesgo: si Clockify falla, ensucia el ciclo de Asana; si tarda mucho, bloquea todo.
   - Recomendación: permitir `ENABLE_CLOCKIFY_SYNC` o `--clockify` flag para habilitar/deshabilitar; registrar tiempos.

---

## P1 — Performance

1) **Consultas repetidas por página**
   - Varias páginas recalculan filtros similares y usan `jsonb_array_elements` en múltiples queries.
   - Recomendación: crear **vistas o CTE reusables** para:
     - `pmo_id`, `sponsor`, `responsable`, `business_vertical`, `fase`, `en_plan_facturacion`.

2) **`jsonb_array_elements` en filtros con `EXISTS`**
   - En grandes datasets es costoso.
   - Recomendación: considerar **columnas derivadas** (materializadas) en tabla `projects` para campos más consultados:
     - `pmo_id`, `sponsor`, `responsable`, `business_vertical`, `fase`.

3) **Pandas styling pesado**
   - `df.style.apply` es costoso en UI grandes.
   - Recomendación: aplicar estilo solo cuando el dataset sea menor a un umbral configurable.

---

## P1 — Usabilidad

1) **Menú lateral**
   - Hoy se divide en Asana/General/Clockify con radios independientes.
   - Recomendación: navegación única para reducir confusión y clicks.

2) **Feedback de filtros**
   - En páginas de búsqueda y seguimiento, sería útil mostrar “filtros aplicados”.
   - Recomendación: agregar `st.caption` con resumen de filtros activos.

3) **Mensajes de error consistentes**
   - Algunos errores muestran `st.error`, otros `st.info`.
   - Recomendación: centralizar un helper para mostrar errores con un formato uniforme.

---

## P1 — Mantenibilidad

1) **Duplicación de queries**
   - Varias páginas repiten filtros Asana (PMO ID, Business Vertical, fase terminada).
   - Recomendación: mover a `ui/lib/queries.py` con funciones como:
     - `base_projects_where(...)`
     - `base_projects_params(...)`

2) **Helpers en `ui/lib/common.py`**
   - Algunos helpers aún están duplicados en páginas Clockify.
   - Recomendación: converger y reutilizar los mismos helpers.

3) **Sincronización de Clockify**
   - Script `run_clockify_sync.py` es autosuficiente, pero muy extenso.
   - Recomendación: separar en módulos:
     - `clockify/api.py`, `clockify/transform.py`, `clockify/db.py`, `clockify/sync.py`.

---

## P2 — Buenas prácticas

1) **Logging**
   - Clockify sync imprime en stdout.
   - Recomendación: usar `logging` y `configure_logging()` similar a Asana.

2) **Configurable thresholds**
   - Clockify sync incremental days está hardcodeado en `run_sync.py`.
   - Recomendación: mover a `config.yaml` (`clockify.incremental_days`).

3) **Tests**
   - Falta cobertura de:
     - Pagos (insert/update/historial)
     - Clockify queries
     - Queries base de Asana
   - Recomendación: agregar tests mínimos de SQL con fixtures de DB local.

---

## P3 — Limpieza

1) **Archivos temporales**
   - Validar que no queden scripts auxiliares en `temp/`.

2) **Consistency de nombres**
   - `Plan de facturación` vs `Facturación`.
   - Recomendación: unificar nomenclatura en UI y documentación.

---

## Siguiente paso sugerido

Priorizar P0 y P1. Recomiendo empezar por:

1) **Unificar helpers de fecha/timezone en `common.py`**
2) **Refactor de filtros base en queries reutilizables**
3) **Un solo selector de navegación en sidebar**

