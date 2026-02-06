# Status Updates & Comments (Asana)

## Requerimiento
Leer **todas las notas de status updates** de proyectos activos (no completados, no TERMINADO, no CANCELADO) y **todos los comentarios** de esos updates.  
Guardar histórico completo en base de datos.  
En UI: mostrar el **update más reciente**, con opción **“Ver más”** para navegar hacia atrás, además de comentarios.

## Implementación
### 1) DB
Se agregaron tablas nuevas en `src/controltower/db/schema.sql`:
- `status_updates`
  - `gid`, `project_gid`, `created_at`, `author_*`, `status_type`, `title`, `text`, `html_text`, `raw_data`, `synced_at`
- `status_update_comments`
  - `status_update_gid`, `story_gid`, `created_at`, `author_*`, `text`, `html_text`, `raw_data`, `synced_at`

### 2) Asana Client
En `src/controltower/asana/client.py`:
- `list_status_updates(project_gid)`  
  Usa `StatusUpdatesApi.get_statuses_for_object` con `opt_fields`.
- `list_status_update_comments(status_update_gid)`  
  Llama `GET /status_updates/{gid}/stories` usando `api_client.call_api`.

### 3) Sync
En `src/controltower/sync/sync_runner.py`:
- Por cada proyecto activo:
  - Se guardan **todos** los status updates.
  - Para cada update se guardan todos los comentarios (stories).
  - Se usa `upsert_status_update` y `insert_status_update_comment`.

### 4) UI (Proyectos)
En `src/controltower/ui/app.py`:
- Se añadió columna **“Updates”** al final de la grilla.
- Cada fila tiene link **“Ver updates”**.
- Al abrir:
  - Muestra **Fecha, Estado, Autor, Texto, Next Steps** y **Comentarios**.
  - Solo el **más reciente** por defecto.
  - Botón **“Ver más”** para navegar hacia atrás.
  - Botón **“Cerrar”** para volver a la grilla.

## Problemas encontrados y soluciones
1) **`RetryError ... AttributeError`** al leer comentarios  
   - Causa: respuesta del SDK variaba (dict/list/objeto).  
   - Solución: normalizar `resp` y devolver lista vacía si el formato no es el esperado.

2) **`'list' object has no attribute 'items'`**  
   - Causa: `query_params` se pasó como **lista** y el SDK esperaba **dict**.  
   - Solución: `query_params` ahora es dict.

3) **Errores de parsing de respuesta**  
   - Se encapsuló con `try/except` y warning en el logger `asana` para evitar caída del sync.

## Estado actual
✅ Requerimiento completo:
- Sync completo de updates y comentarios.
- Persistencia en DB.
- UI con navegación del update más reciente hacia atrás.

## Último cambio aplicado
**Fix en `list_status_update_comments`**:
`query_params` pasó de lista a **dict**, eliminando el error:
`'list' object has no attribute 'items'`.

