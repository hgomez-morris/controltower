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
              completed_flag = COALESCE(completed_flag, (p.raw_data->'project'->>'completed')::boolean)
            WHERE p.raw_data ? 'project';
        """))


if __name__ == "__main__":
    main()
