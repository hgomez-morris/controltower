# PMO Control Tower (MVP)

Capa de observación, control y alertas para proyectos gestionados en Asana.
Este MVP es **read-only** respecto de Asana: solo consulta datos y genera hallazgos/alertas.

## Quickstart (dev local)

1) Clonar el repo (vacío inicialmente) en:
`C:\MorrisFiles\Proyectos\ControlTower`

2) Crear `.env` desde el ejemplo:
- `.env.example` -> `.env`

3) Levantar PostgreSQL local:
```bash
docker compose up -d
```

4) Instalar dependencias:
```bash
python -m venv .venv
# Windows PowerShell:
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

5) Crear esquema DB:
```bash
python scripts/init_db.py
```

6) Ejecutar un sync manual:
```bash
python scripts/run_sync.py
```

6b) (Opcional) Sync paralelo:
```bash
python scripts/run_sync_parallel.py
```

6c) (Opcional) Cargar históricos (una sola vez):
```bash
python scripts/load_projects_history.py
```

7) Abrir UI:
```bash
streamlit run src/controltower/ui/app.py
```

## Alcance MVP
- Sync (cada 2h en prod, manual en dev)
- 3 reglas PMO (no_status_update, no_activity, schedule_risk)
- Alertas Slack (solo nuevos hallazgos)
- UI Streamlit
- KPI (tabs KPI1/KPI2/KPI3) con snapshots en DB
- Búsqueda local (prioriza tabla sincronizada y cae a históricos)

## No-alcance MVP
- Modificar Asana (crear/editar tareas/proyectos) — Prohibido
- Auth Cognito
- UI React
