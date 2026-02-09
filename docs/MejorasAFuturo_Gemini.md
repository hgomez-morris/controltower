# Análisis y Propuesta de Mejoras - PMO Control Tower

**Autor:** Gemini Agent  
**Fecha:** 5 de Febrero, 2026  
**Contexto:** Análisis realizado tras estabilizar el núcleo de sincronización con Asana.

---

## 1. Resumen Ejecutivo (Estado Actual)

La aplicación **Control Tower** cumple actualmente con una función vital para una PMO: **Centralización y Consolidación**.

**Fortalezas Actuales:**
*   **Agnosticismo de Fuente:** Abstrae la lógica de Asana, permitiendo una base de datos normalizada propia.
*   **Auditoría de Cambios:** El `project_changelog` es una herramienta potente para ver la "derivada" del proyecto, no solo la foto actual.
*   **Motor de Reglas (Incípiente):** La estructura para detectar `findings` (hallazgos) ya existe, permitiendo auditoría automática.
*   **Visibilidad de Status:** La reciente incorporación de Updates y Comentarios cierra el ciclo de comunicación.

**Oportunidad:**
Transformar la herramienta de un **"Visor Pasivo"** a un **"Motor de Gobernanza Activo"**. Pasar de "¿Cómo vamos?" a "Aquí están los riesgos y esto es lo que debemos hacer".

---

## 2. Propuestas de Mejora (Perspectiva PMO)

Se proponen mejoras divididas en 4 pilares estratégicos: **Inteligencia**, **Gobernanza**, **Visualización** y **Operación**.

### Pilar 1: Inteligencia y Análisis Predictivo 🧠

El objetivo es detectar inconsistencias entre lo que el PM "dice" y lo que los datos "muestran".

#### 1.1 Análisis de Sentimiento y Coherencia (AI Integration)
*   **Problema:** Un proyecto puede tener semáforo "Verde", pero el texto del update estar lleno de palabras como "bloqueo", "retraso", "riesgo".
*   **Solución:** Integrar un LLM (vía API) para analizar el `text` del último Status Update.
    *   **Feature:** "Semáforo Calculado vs. Semáforo Declarado".
    *   **Output:** Una alerta si el sentimiento es negativo pero el status es verde (Sandía: verde por fuera, rojo por dentro).

#### 1.2 Tendencias Históricas (Trend Analysis)
*   **Problema:** Ver el % de avance hoy no nos dice si el equipo está acelerando o frenando.
*   **Solución:** Calcular la velocidad del proyecto basándose en los snapshots diarios/semanales.
    *   **Gráfico:** Burn-up chart calculado automáticamente (Total Tasks vs Completed Tasks en el tiempo).
    *   **Métrica:** "Días estimados para finalización" basado en la velocidad real de las últimas 4 semanas.

---

### Pilar 2: Gobernanza y Estandarización rules 📐

Fortalecer el motor de reglas (`rules/engine.py`) para asegurar la higiene de los datos.

#### 2.1 SLAs de Comunicación
*   **Mejora:** Regla configurable de "Frescura del Dato".
*   **Detalle:** Si un proyecto activo no tiene un *Status Update* en los últimos 7 días (o viernes por la tarde), generar un `finding` de severidad ALTA.
*   **Acción:** Notificación automática al owner.

#### 2.2 Validación de Fechas (Due Date Integrity)
*   **Mejora:** Detección de "Due Date Sliding".
*   **Detalle:** Usar el `project_changelog` para detectar cuántas veces se ha movido la fecha de fin. Si se mueve más de 3 veces, marcar como "Proyecto en Riesgo de Planificación".

#### 2.3 Auditoría de Campos Obligatorios
*   **Mejora:** Verificar completitud de metadatos.
*   **Detalle:** Asegurar que campos personalizados críticos (ej. "Presupuesto", "Cliente", "Prioridad Estratégica") no estén vacíos.

---

### Pilar 3: Visualización Estratégica (Dashboarding) 📊

La UI actual es funcional (tabular), pero necesita vistas para diferentes audiencias.

#### 3.1 Vista de Portafolio / Ejecutiva
*   **Concepto:** Los directores no quieren ver una lista de 50 proyectos. Quieren ver agregados.
*   **Features:**
    *   Dona de estados (X% Verde, Y% Rojo).
    *   Top 5 Proyectos Críticos (basado en severidad de findings).
    *   Matriz de Riesgos (Impacto vs Probabilidad, si se extrae esa info).

#### 3.2 "Focus Mode" para PMs
*   **Concepto:** "Mi día a día".
*   **Features:** Un filtro rápido "Mis Proyectos" que muestre solo donde soy Owner.
*   **Acción:** Botones rápidos para resolver `findings` (ej. "Ya actualicé Asana", "Falso positivo").

#### 3.3 Línea de Tiempo (Gantt de Alto Nivel)
*   **Concepto:** Visualizar solapamientos y fechas de entrega macro.
*   **Tech:** Usar componentes de timeline de Streamlit para dibujar `start_on` y `due_on` de los proyectos.

---

### Pilar 4: Operación y Automatización ⚙️

Hacer que el sistema trabaje por el PMO.

#### 4.1 Notificaciones Interactivas (SlackOps)
*   **Mejora:** Evolucionar `src/controltower/actions/slack.py`.
*   **Flujo:**
    1.  Sync corre a las 8:00 AM.
    2.  Detecta que el proyecto "Migración Cloud" no tiene update semanal.
    3.  Envía mensaje directo a Slack del Owner: *"Hola, tu proyecto no tiene update. Por favor actualízalo antes de las 12:00."*
    4.  Incluir link directo al proyecto en Asana.

#### 4.2 Generación de Borradores de Status (Assisted Reporting)
*   **Mejora:** Ayudar al PM a escribir el reporte.
*   **Detalle:** En la UI, un botón "Generar Borrador". El sistema lee las tareas completadas de la semana y redacta un resumen: *"Esta semana se completaron 5 tareas, incluyendo X e Y. El progreso subió un 2%."*

---

## 3. Priorización Sugerida (Roadmap)

| Fase | Enfoque | Features Clave | Esfuerzo | Impacto |
| :--- | :--- | :--- | :--- | :--- |
| **Q1 (Corto Plazo)** | **Higiene y Visibilidad** | SLAs de Status (Reglas), Dashboard Ejecutivo Básico, Filtros en UI. | Bajo | Alto |
| **Q2 (Mediano Plazo)** | **Proactividad** | Integración Slack (Notificaciones), Detección de "Due Date Sliding". | Medio | Medio |
| **Q3 (Largo Plazo)** | **Inteligencia** | Análisis de Sentimiento con IA, Predicción de fechas de fin. | Alto | Alto |

## 4. Conclusión Técnica

El código actual es modular y permite estas extensiones sin reescribir el núcleo.
*   **BD:** El esquema soporta bien las nuevas métricas (solo faltaría una tabla de `daily_snapshots` para tendencias más finas).
*   **Sync:** Está robusto.
*   **UI:** Streamlit es flexible para prototipar estos dashboards rápidamente.

Esta evolución convertirá a **Control Tower** en una herramienta indispensable para la toma de decisiones basada en datos reales, reduciendo la carga administrativa de la PMO.
