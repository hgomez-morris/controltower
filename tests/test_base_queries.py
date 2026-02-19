from __future__ import annotations

from controltower.ui.lib.queries import base_projects_params, base_projects_where


def test_base_projects_where_defaults():
    clauses = base_projects_where(table_alias="p")
    joined = " ".join(clauses).lower()
    assert "pmo id" in joined
    assert "business vertical" in joined
    assert "fase del proyecto" in joined
    assert "completed" in joined


def test_base_projects_params_includes_sponsor_and_bv():
    params = base_projects_params("Abrigo", "Professional Services")
    assert params["sponsor"] == "Abrigo"
    assert params["sponsor_like"] == "%Abrigo%"
    assert params["bv"] == "Professional Services"
    assert params["bv_like"] == "%Professional Services%"
