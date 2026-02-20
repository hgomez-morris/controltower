# Predicción de Atraso Temprano en Proyectos

## Objetivo
Implementar un sistema de Machine Learning que prediga, usando solo las primeras semanas de ejecución, si un proyecto terminará con una desviación mayor al 20% respecto a sus horas planificadas.

El sistema debe:
- Entrenarse con proyectos cerrados.
- Ejecutarse semanalmente.
- Guardar resultados en PostgreSQL.
- Exponerse en una nueva página dentro de la aplicación Streamlit.

Este documento está diseñado para ser utilizado directamente por Codex para implementar la funcionalidad.

---

# 1. Definiciones de Negocio

## 1.1 Proyecto Activo
Un proyecto se considera activo si:
- `completed_flag = false`
- `fase_proyecto NOT IN ('TERMINADO','CANCELADO')`

## 1.2 Proyecto Cerrado (para entrenamiento)
Un proyecto se considera válido para entrenamiento si:
- `completed_flag = true`
- `fase_proyecto = 'TERMINADO'`

Los proyectos CANCELADOS se excluyen del modelo.

## 1.3 Label (Variable Objetivo)

Desviación final:

( HorasRealesTotales - HorasPlanificadas ) / HorasPlanificadas

Label = 1 si desviación > 0.20
Label = 0 en caso contrario

---

# 2. Fuentes de Datos

## 2.1 Asana (Tabla projects)
Campos relevantes:
- pmo_id
- owner_gid
- due_date
- completed_flag
- fase_proyecto
- planned_hours_total (campo obligatorio para el modelo)

## 2.2 Clockify

### clockify.projects
- id
- name (formato: "#PMO-XXXX Nombre")

### clockify.time_entries
- project_id
- user_id
- entry_date
- hours
- updated_at

### clockify.calendar_weeks
- week_start
- week_end

---

# 3. Mapeo entre Asana y Clockify

1. Extraer PMO-ID desde clockify.projects.name usando regex:
   "#(PMO-\\d+)"

2. Hacer match con projects.pmo_id

Este match debe guardarse en una tabla auxiliar:

## project_id_map
- pmo_id
- asana_project_gid
- clockify_project_id

---

# 4. Construcción del Dataset Analítico

## 4.1 Tabla weekly_fact

Generar tabla agregada por proyecto y semana:

Campos:
- pmo_id
- week_start
- hours_week
- active_users_week

hours_week = SUM(hours)
active_users_week = COUNT(DISTINCT user_id)

---

## 4.2 Definición de Semana
Semanas de lunes a domingo (zona horaria Chile).

---

# 5. Snapshots para ML

Se generan snapshots usando solo las primeras k semanas:
- k = 2
- k = 3
- k = 4

Cada fila del dataset ML representa:
(pmo_id, k)

---

# 6. Features

## 6.1 Duración planificada
D = ceil( (due_date - start_date) / 7 )

Si no existe start_date explícito:
start_date = MIN(entry_date)

---

## 6.2 Horas esperadas por semana
H_exp = planned_hours_total / D

---

## 6.3 Ratio de consumo temprano
ratio_burn = H_real_k / (k * H_exp)

---

## 6.4 Pendiente de consumo
Regresión lineal sobre horas_week (semanas 1..k)
Feature: slope_hours_week

---

## 6.5 Volatilidad
volatility_hours = std(H1..Hk) / mean(H1..Hk)

---

## 6.6 Fragmentación
- active_people_k
- people_growth
- hours_top1_share

---

## 6.7 Carga del Responsable
Parámetro configurable:
weekly_capacity_hours DEFAULT = 45
Rango permitido: 40–48

Features:
- jp_active_projects_k
- jp_total_hours_k
- jp_utilization = jp_total_hours_k / (k * weekly_capacity_hours)

---

# 7. Baseline

Reglas iniciales:
- Riesgo si ratio_burn > 1.2
- Riesgo si ratio_burn > 1.1 y slope > 0

El modelo ML debe superar este baseline.

---

# 8. Modelo ML

Modelo recomendado: XGBoost (clasificación binaria)

Entrenamiento:
- Split temporal (evitar leakage)
- Usar solo proyectos cerrados
- Entrenar modelos independientes para k=2,3,4

Métricas:
- Precision@TopN
- Recall clase positiva
- ROC-AUC

Umbral sugerido inicial: 0.70

---

# 9. Tablas Nuevas en PostgreSQL

## ml_project_labels
- pmo_id
- final_deviation
- label

## ml_project_features
- pmo_id
- k
- ratio_burn
- slope_hours_week
- volatility_hours
- active_people_k
- jp_utilization
- log_planned_hours

## ml_project_scores
- pmo_id
- k
- probability
- scoring_date

---

# 10. Pipeline Semanal

Frecuencia: semanal

Pasos:
1. Extraer nuevas imputaciones
2. Actualizar weekly_fact
3. Recalcular features para proyectos activos
4. Aplicar modelo correspondiente según semanas transcurridas
5. Insertar resultados en ml_project_scores
6. Exponer en Streamlit

Nota:
Como las horas pueden editarse hasta 60 días atrás, se deben recalcular últimas 9 semanas completas en cada corrida.

---

# 11. Visualización en Streamlit

Página nueva: "Predicción de Riesgo"

Mostrar:
- Top 20 proyectos con mayor probabilidad
- Filtros por responsable
- Filtros por k
- Probabilidad
- Drivers principales

---

# 12. Checklist de Calidad de Datos

Excluir proyectos si:
- planned_hours_total IS NULL
- planned_hours_total = 0
- due_date IS NULL
- No tienen imputaciones

Loggear en tabla:
ml_data_quality_issues

---

# 13. Estructura Recomendada del Repo

/src
  /etl
  /features
  /training
  /inference
  /utils
  config.py

/models

/jobs

---

# 14. Resultado Esperado

Cada lunes el sistema debe producir una tabla actualizada con probabilidades de riesgo para todos los proyectos activos.

Gerencia debe poder ver:
"Estos N proyectos tienen alta probabilidad de terminar con más de 20% de desviación."

---

FIN DEL DOCUMENTO

