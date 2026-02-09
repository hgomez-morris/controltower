# Roadmap de Evolución: Control Tower (Consolidado)

**Versión:** 1.0 (Consolidada)  
**Fecha:** 5 de Febrero, 2026  
**Resumen:** Documento maestro que integra la visión de procesos PMO con la modernización tecnológica y automatización inteligente.

---

## 1. Visión Estratégica
Transformar la Control Tower de un **"Radiador de Información"** a un **"Motor de Gobernanza Activo"**. La herramienta no solo debe mostrar datos, sino facilitar el flujo de trabajo (workflow) para resolver desviaciones y predecir riesgos antes de que se materialicen.

---

## 2. Pilares de Evolución

### Pilar 1: Gobernanza y Procesos (The PMO Core)
*Objetivo: Establecer las reglas del juego y asegurar la integridad de la gestión.*

1.  **Estado PMO "Shadow" (Ciclo de Vida Interno):**
    *   *Concepto:* Desacoplar el estado técnico de Asana del estado de gestión PMO.
    *   *Estados:* `Inicio` → `En Ejecución` → `En Riesgo` (Manual/Auto) → `En Pausa` → `Cerrado`.
    *   *Valor:* Permite a la PMO marcar un proyecto como "En Riesgo" aunque en Asana figure en verde, forzando un plan de acción.

2.  **Matriz de Severidad Dinámica:**
    *   *Lógica:* La severidad de un hallazgo no es estática.
    *   *Fórmula:* `Severidad Base` + `Criticidad Cliente` + `Presupuesto` + `Reglas Activas Simultáneas`.
    *   *Resultado:* Un proyecto con 3 alertas leves se convierte en PRIORIDAD ALTA automáticamente.

3.  **SLAs de Reporting (Reglas de Frescura):**
    *   **Regla:** `freshness_check`. Si `last_update > 7 días` (o viernes PM) → Alerta.
    *   **Regla:** `due_date_sliding`. Detección de cambios reiterados en fecha fin (> 3 veces).
    *   **Regla:** `data_integrity`. Campos obligatorios vacíos (Sponsor, Presupuesto, Cliente).

### Pilar 2: Flujo de Trabajo y Cierre de Loop (Workflow)
*Objetivo: Que los hallazgos (findings) se resuelvan, no solo se listen.*

1.  **Gestión de Hallazgos "Actionable":**
    *   *Feature:* Cada *finding* debe permitir:
        *   **Acknowledge:** "Lo vi, estoy en ello".
        *   **Snooze:** "Ocultar por 1 semana".
        *   **Resolve:** "Corregido".
        *   **False Positive:** "Esta regla no aplica aquí".
    *   *Plan de Acción:* Campo de texto obligatorio al reconocer un hallazgo crítico.

2.  **Automatización Operativa (SlackOps):**
    *   *Integración:* Bot de Slack bidireccional.
    *   *Push:* "Hola [Owner], tu proyecto [X] no tiene update esta semana."
    *   *Pull:* El usuario puede responder "/ack" directamente desde Slack.

### Pilar 3: Inteligencia y Predicción (AI & Analytics)
*Objetivo: Leer entre líneas lo que los datos crudos no dicen.*

1.  **Análisis de Sentimiento (Audit):**
    *   *IA:* Usar LLM para analizar el texto de los *Status Updates*.
    *   *Detección:* "Efecto Sandía" (Semáforo Verde declarado vs. Texto con palabras de riesgo/bloqueo).
    *   *Output:* Alerta de incoherencia.

2.  **Generación de Borradores (Assisted Reporting):**
    *   *IA:* Generar draft de reporte semanal basado en tareas completadas/creadas de la semana.
    *   *Beneficio:* Reduce la fricción del PM para reportar.

3.  **Tendencias (Burn-up & Velocity):**
    *   *Analytics:* Gráficos de velocidad real vs. planificada basados en snapshots históricos.

### Pilar 4: Experiencia de Usuario (UX/UI)
*Objetivo: Vistas adaptadas al rol del usuario.*

1.  **Inbox del Responsable (Focus Mode):**
    *   Vista filtrada: "Mis Proyectos" + "Mis Hallazgos Pendientes".
    *   Elimina el ruido de ver todo el portafolio.

2.  **Dashboard Ejecutivo (High Level):**
    *   KPIs agregados: % Cumplimiento de Reporting, Top 5 Proyectos Críticos, Distribución de Estados.
    *   Sin tablas detalladas, solo métricas de impacto.

3.  **Línea de Tiempo (Gantt de Portafolio):**
    *   Visualización de `start_on` y `due_date` para identificar cuellos de botella de recursos.

---

## 3. Roadmap de Implementación

### Fase 1: Higiene y Control (Corto Plazo - Mes 1)
*Enfoque: Asegurar que los datos sean confiables y el ciclo de revisión funcione.*
1.  **Refinamiento de Reglas:** Implementar exclusiones (proyectos sin tareas) y nuevas reglas de integridad (campos vacíos).
2.  **UI de Hallazgos:** Implementar acciones básicas (Acknowledge/Resolve) en la base de datos y UI.
3.  **Inbox de Responsable:** Filtro rápido por Owner en la UI actual.

### Fase 2: Proactividad y Comunicación (Mediano Plazo - Mes 2)
*Enfoque: Sacar la herramienta del navegador y llevarla al flujo de trabajo.*
1.  **Slack Integration V1:** Notificaciones unidireccionales de SLAs incumplidos.
2.  **Matriz de Severidad:** Implementar lógica de peso para priorizar alertas.
3.  **Dashboard Ejecutivo:** Primera versión de métricas agregadas.

### Fase 3: Inteligencia y Madurez (Largo Plazo - Mes 3+)
*Enfoque: Valor añadido mediante IA y análisis avanzado.*
1.  **AI Sentiment Analysis:** Comparativa Semáforo vs. Texto.
2.  **Predicción de Fechas:** Análisis de tendencias históricas.
3.  **Estado PMO Shadow:** Implementación completa del ciclo de vida paralelo.

---

## 4. Notas Técnicas de Implementación

*   **Stack:** Mantener Python/Streamlit para velocidad.
*   **Base de Datos:** El esquema actual (`findings`, `projects`) soporta la mayoría de cambios. Se requerirá una tabla `findings_actions` para el historial de resoluciones.
*   **IA:** Integración vía API (OpenAI/Gemini/Claude) encapsulada en servicio independiente para el análisis de texto.
