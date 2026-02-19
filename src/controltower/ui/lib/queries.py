from __future__ import annotations


def base_projects_where(
    table_alias: str = "p",
    sponsor_filter: str | None = None,
    bv_filter: str | None = None,
    require_pmo: bool = True,
    require_bv_ps: bool = True,
    include_terminated: bool = False,
    include_completed: bool = False,
) -> list[str]:
    alias = table_alias
    clauses: list[str] = []

    if require_pmo:
        clauses.append(
            f"""
            EXISTS (
              SELECT 1 FROM jsonb_array_elements({alias}.raw_data->'project'->'custom_fields') cf
              WHERE cf->>'name' = 'PMO ID' AND COALESCE(cf->>'display_value','') <> ''
            )
            """
        )

    if require_bv_ps:
        clauses.append(
            f"""
            EXISTS (
              SELECT 1 FROM jsonb_array_elements({alias}.raw_data->'project'->'custom_fields') cf_bv
              WHERE (cf_bv->>'gid' = '1209701308000267' OR cf_bv->>'name' = 'Business Vertical')
                AND (
                  (cf_bv->'enum_value'->>'gid') = '1209701308000273'
                  OR (cf_bv->'enum_value'->>'name') = 'Professional Services'
                  OR COALESCE(cf_bv->>'display_value','') = 'Professional Services'
                )
            )
            """
        )

    if not include_terminated:
        clauses.append(
            f"""
            NOT EXISTS (
              SELECT 1 FROM jsonb_array_elements({alias}.raw_data->'project'->'custom_fields') cf_phase
              WHERE (cf_phase->>'gid' = '1207505889399747' OR cf_phase->>'name' = 'Fase del proyecto')
                AND (
                  lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%terminad%'
                  OR lower(COALESCE(cf_phase->>'display_value', cf_phase->'enum_value'->>'name','')) LIKE '%cancelad%'
                )
            )
            """
        )

    if not include_completed:
        clauses.append(f"COALESCE({alias}.raw_data->'project'->>'completed','false') <> 'true'")

    if sponsor_filter is not None:
        clauses.append(
            f"""
            (:sponsor = '' OR EXISTS (
              SELECT 1 FROM jsonb_array_elements({alias}.raw_data->'project'->'custom_fields') cf_s
              WHERE cf_s->>'name' = 'Sponsor' AND COALESCE(cf_s->>'display_value','') ILIKE :sponsor_like
            ))
            """
        )

    if bv_filter is not None:
        clauses.append(
            f"""
            (:bv = '' OR EXISTS (
              SELECT 1 FROM jsonb_array_elements({alias}.raw_data->'project'->'custom_fields') cf_bv2
              WHERE (cf_bv2->>'gid' = '1209701308000267' OR cf_bv2->>'name' = 'Business Vertical')
                AND COALESCE(cf_bv2->>'display_value', cf_bv2->'enum_value'->>'name','') ILIKE :bv_like
            ))
            """
        )

    return clauses


def base_projects_params(sponsor_filter: str | None = None, bv_filter: str | None = None) -> dict:
    sponsor = (sponsor_filter or "").strip()
    bv = (bv_filter or "").strip()
    return {
        "sponsor": sponsor,
        "sponsor_like": f"%{sponsor}%" if sponsor else "%",
        "bv": bv,
        "bv_like": f"%{bv}%" if bv else "%",
    }
