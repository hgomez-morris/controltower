from __future__ import annotations

import os
from sqlalchemy import text
from controltower.db.connection import get_engine


def main() -> None:
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE projects p
            SET
              pmo_id = COALESCE(pmo_id, (
                SELECT COALESCE(cf->>'display_value','')
                FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                WHERE cf->>'name' = 'PMO ID'
                LIMIT 1
              )),
              sponsor = COALESCE(sponsor, (
                SELECT COALESCE(cf->>'display_value','')
                FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                WHERE cf->>'name' = 'Sponsor'
                LIMIT 1
              )),
              cliente_nuevo = COALESCE(cliente_nuevo, (
                SELECT COALESCE(cf->>'display_value', cf->'enum_value'->>'name','')
                FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                WHERE lower(COALESCE(cf->>'name','')) IN ('cliente_nuevo','cliente nuevo')
                LIMIT 1
              )),
              tipo_proyecto = COALESCE(tipo_proyecto, (
                SELECT COALESCE(cf->>'display_value', cf->'enum_value'->>'name','')
                FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                WHERE lower(COALESCE(cf->>'name','')) IN ('tipo de proyecto','tipo proyecto')
                LIMIT 1
              )),
              clasificacion = COALESCE(clasificacion, (
                SELECT COALESCE(cf->>'display_value', cf->'enum_value'->>'name','')
                FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                WHERE lower(COALESCE(cf->>'name','')) IN ('clasificación','clasificacion')
                LIMIT 1
              )),
              segmento_empresa = COALESCE(segmento_empresa, (
                SELECT COALESCE(cf->>'display_value', cf->'enum_value'->>'name','')
                FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                WHERE lower(COALESCE(cf->>'name','')) LIKE 'segmento%empresa%'
                   OR lower(COALESCE(cf->>'name','')) = 'segmento'
                LIMIT 1
              )),
              pais = COALESCE(pais, (
                SELECT COALESCE(cf->>'display_value', cf->'enum_value'->>'name','')
                FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                WHERE lower(COALESCE(cf->>'name','')) IN ('país','pais')
                LIMIT 1
              )),
              responsable_proyecto = COALESCE(responsable_proyecto, (
                SELECT COALESCE(cf->>'display_value','')
                FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                WHERE cf->>'name' = 'Responsable Proyecto'
                LIMIT 1
              )),
              business_vertical = COALESCE(business_vertical, (
                SELECT COALESCE(cf->>'display_value', cf->'enum_value'->>'name','')
                FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                WHERE (cf->>'gid' = '1209701308000267' OR cf->>'name' = 'Business Vertical')
                LIMIT 1
              )),
              fase_proyecto = COALESCE(fase_proyecto, (
                SELECT COALESCE(cf->>'display_value', cf->'enum_value'->>'name','')
                FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                WHERE (cf->>'gid' = '1207505889399747' OR cf->>'name' = 'Fase del proyecto')
                LIMIT 1
              )),
              en_plan_facturacion = COALESCE(en_plan_facturacion, (
                SELECT CASE
                    WHEN lower(trim(COALESCE(cf->>'display_value',''))) IN ('si','sí') THEN TRUE
                    ELSE FALSE
                END
                FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                WHERE lower(COALESCE(cf->>'name','')) LIKE 'en plan de fact%'
                LIMIT 1
              )),
              completed_flag = COALESCE(completed_flag, (p.raw_data->'project'->>'completed')::boolean),
              start_date = COALESCE(start_date, (
                SELECT COALESCE(cf->'date_value'->>'date', cf->>'display_value')::date
                FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                WHERE cf->>'gid' = '1207505889399729' OR lower(cf->>'name') = 'fecha inicio del proyecto'
                LIMIT 1
              )),
              planned_end_date = COALESCE(
                planned_end_date,
                (
                  SELECT COALESCE(cf->'date_value'->>'date', cf->>'display_value')::date
                  FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                  WHERE cf->>'gid' = '1207505889399731' OR lower(cf->>'name') = 'fecha planificada termino del proyecto'
                  LIMIT 1
                ),
                NULLIF(p.raw_data->'project'->>'due_date', '')::date,
                NULLIF(p.raw_data->'project'->>'due_on', '')::date
              ),
              planned_hours_total = COALESCE(planned_hours_total, (
                SELECT NULLIF(replace(regexp_replace(COALESCE(cf->>'display_value', cf->>'number_value', cf->>'text_value',''), '[^0-9.,-]', '', 'g'), ',', '.'), '')
                FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                WHERE cf->>'gid' = '1207505889399760' OR lower(cf->>'name') IN ('horas planificadas','horas planificada')
                LIMIT 1
              )::decimal),
              effective_hours_total = COALESCE(effective_hours_total, (
                SELECT NULLIF(replace(regexp_replace(COALESCE(cf->>'display_value', cf->>'number_value', cf->>'text_value',''), '[^0-9.,-]', '', 'g'), ',', '.'), '')
                FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                WHERE cf->>'gid' = '1207505889399792' OR lower(cf->>'name') IN ('horas efectivas','horas efectivas ')
                LIMIT 1
              )::decimal)
            WHERE p.raw_data ? 'project';
        """))

        conn.execute(text("""
            UPDATE projects_history p
            SET
              pmo_id = COALESCE(pmo_id, (
                SELECT COALESCE(cf->>'display_value','')
                FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                WHERE cf->>'name' = 'PMO ID'
                LIMIT 1
              )),
              sponsor = COALESCE(sponsor, (
                SELECT COALESCE(cf->>'display_value','')
                FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                WHERE cf->>'name' = 'Sponsor'
                LIMIT 1
              )),
              cliente_nuevo = COALESCE(cliente_nuevo, (
                SELECT COALESCE(cf->>'display_value', cf->'enum_value'->>'name','')
                FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                WHERE lower(COALESCE(cf->>'name','')) IN ('cliente_nuevo','cliente nuevo')
                LIMIT 1
              )),
              tipo_proyecto = COALESCE(tipo_proyecto, (
                SELECT COALESCE(cf->>'display_value', cf->'enum_value'->>'name','')
                FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                WHERE lower(COALESCE(cf->>'name','')) IN ('tipo de proyecto','tipo proyecto')
                LIMIT 1
              )),
              clasificacion = COALESCE(clasificacion, (
                SELECT COALESCE(cf->>'display_value', cf->'enum_value'->>'name','')
                FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                WHERE lower(COALESCE(cf->>'name','')) IN ('clasificación','clasificacion')
                LIMIT 1
              )),
              segmento_empresa = COALESCE(segmento_empresa, (
                SELECT COALESCE(cf->>'display_value', cf->'enum_value'->>'name','')
                FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                WHERE lower(COALESCE(cf->>'name','')) LIKE 'segmento%empresa%'
                   OR lower(COALESCE(cf->>'name','')) = 'segmento'
                LIMIT 1
              )),
              pais = COALESCE(pais, (
                SELECT COALESCE(cf->>'display_value', cf->'enum_value'->>'name','')
                FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                WHERE lower(COALESCE(cf->>'name','')) IN ('país','pais')
                LIMIT 1
              )),
              responsable_proyecto = COALESCE(responsable_proyecto, (
                SELECT COALESCE(cf->>'display_value','')
                FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                WHERE cf->>'name' = 'Responsable Proyecto'
                LIMIT 1
              )),
              business_vertical = COALESCE(business_vertical, (
                SELECT COALESCE(cf->>'display_value', cf->'enum_value'->>'name','')
                FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                WHERE (cf->>'gid' = '1209701308000267' OR cf->>'name' = 'Business Vertical')
                LIMIT 1
              )),
              fase_proyecto = COALESCE(fase_proyecto, (
                SELECT COALESCE(cf->>'display_value', cf->'enum_value'->>'name','')
                FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                WHERE (cf->>'gid' = '1207505889399747' OR cf->>'name' = 'Fase del proyecto')
                LIMIT 1
              )),
              en_plan_facturacion = COALESCE(en_plan_facturacion, (
                SELECT CASE
                    WHEN lower(trim(COALESCE(cf->>'display_value',''))) IN ('si','sí') THEN TRUE
                    ELSE FALSE
                END
                FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                WHERE lower(COALESCE(cf->>'name','')) LIKE 'en plan de fact%'
                LIMIT 1
              )),
              completed_flag = COALESCE(completed_flag, (p.raw_data->'project'->>'completed')::boolean),
              start_date = COALESCE(start_date, (
                SELECT COALESCE(cf->'date_value'->>'date', cf->>'display_value')::date
                FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                WHERE cf->>'gid' = '1207505889399729' OR lower(cf->>'name') = 'fecha inicio del proyecto'
                LIMIT 1
              )),
              planned_end_date = COALESCE(
                planned_end_date,
                (
                  SELECT COALESCE(cf->'date_value'->>'date', cf->>'display_value')::date
                  FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                  WHERE cf->>'gid' = '1207505889399731' OR lower(cf->>'name') = 'fecha planificada termino del proyecto'
                  LIMIT 1
                ),
                NULLIF(p.raw_data->'project'->>'due_date', '')::date,
                NULLIF(p.raw_data->'project'->>'due_on', '')::date
              ),
              planned_hours_total = COALESCE(planned_hours_total, (
                SELECT NULLIF(replace(regexp_replace(COALESCE(cf->>'display_value', cf->>'number_value', cf->>'text_value',''), '[^0-9.,-]', '', 'g'), ',', '.'), '')
                FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                WHERE cf->>'gid' = '1207505889399760' OR lower(cf->>'name') IN ('horas planificadas','horas planificada')
                LIMIT 1
              )::decimal),
              effective_hours_total = COALESCE(effective_hours_total, (
                SELECT NULLIF(replace(regexp_replace(COALESCE(cf->>'display_value', cf->>'number_value', cf->>'text_value',''), '[^0-9.,-]', '', 'g'), ',', '.'), '')
                FROM jsonb_array_elements(p.raw_data->'project'->'custom_fields') cf
                WHERE cf->>'gid' = '1207505889399792' OR lower(cf->>'name') IN ('horas efectivas','horas efectivas ')
                LIMIT 1
              )::decimal)
            WHERE p.raw_data ? 'project';
        """))


if __name__ == "__main__":
    main()
