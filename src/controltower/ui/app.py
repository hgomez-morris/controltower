import streamlit as st

from controltower.ui.ui_pages import (
    busqueda,
    clockify_por_proyectos,
    clockify_por_usuario,
    dashboard,
    findings,
    kpi,
    mensajes,
    pagos,
    plan_facturacion,
    prediccion_riesgo,
    proyectos,
    seguimiento,
)
from controltower.ui.lib.sidebar import apply_sidebar_style, render_sidebar_footer


def main():
    st.set_page_config(page_title="PMO Control Tower (MVP)", layout="wide")
    apply_sidebar_style()

    groups = {
        "Asana": [
            ("Dashboard", dashboard.render),
            ("Proyectos", proyectos.render),
            ("Findings", findings.render),
            ("Mensajes", mensajes.render),
            ("Seguimiento", seguimiento.render),
        ],
        "General": [
            ("KPI", kpi.render),
            ("Búsqueda", busqueda.render),
            ("Facturación", plan_facturacion.render),
            ("Pagos", pagos.render),
        ],
        "Clockify": [
            ("Por Usuario", clockify_por_usuario.render),
            ("Por Proyectos", clockify_por_proyectos.render),
        ],
        "Análisis": [
            ("Predicción de Riesgo", prediccion_riesgo.render),
        ],
    }

    def _set_nav_selected(group_key, other_keys):
        st.session_state["nav_selected"] = st.session_state.get(group_key)
        st.session_state["nav_group"] = group_key
        for k in other_keys:
            st.session_state[k] = None

    if "nav_selected" not in st.session_state or not st.session_state.get("nav_selected"):
        st.session_state["nav_selected"] = groups["Asana"][0][0]
        st.session_state["nav_group"] = "nav_asana"

    with st.sidebar:
        st.markdown("**Asana**")
        asana_options = [t for t, _ in groups["Asana"]]
        asana_selected = st.session_state.get("nav_selected")
        if st.session_state.get("nav_group") != "nav_asana" or asana_selected not in asana_options:
            asana_index = 0
        else:
            asana_index = asana_options.index(asana_selected)
        asana_choice = st.radio(
            "Asana",
            asana_options,
            label_visibility="collapsed",
            key="nav_asana",
            index=asana_index,
            on_change=_set_nav_selected,
            args=("nav_asana", ["nav_general", "nav_clockify", "nav_analysis"]),
        )
        st.markdown("**General**")
        general_options = [t for t, _ in groups["General"]]
        general_selected = st.session_state.get("nav_selected")
        if st.session_state.get("nav_group") != "nav_general" or general_selected not in general_options:
            general_index = None
        else:
            general_index = general_options.index(general_selected)
        general_choice = st.radio(
            "General",
            general_options,
            label_visibility="collapsed",
            key="nav_general",
            index=general_index,
            on_change=_set_nav_selected,
            args=("nav_general", ["nav_asana", "nav_clockify", "nav_analysis"]),
        )
        st.markdown("**Clockify**")
        clockify_options = [t for t, _ in groups["Clockify"]]
        clockify_selected = st.session_state.get("nav_selected")
        if st.session_state.get("nav_group") != "nav_clockify" or clockify_selected not in clockify_options:
            clockify_index = None
        else:
            clockify_index = clockify_options.index(clockify_selected)
        clockify_choice = st.radio(
            "Clockify",
            clockify_options,
            label_visibility="collapsed",
            key="nav_clockify",
            index=clockify_index,
            on_change=_set_nav_selected,
            args=("nav_clockify", ["nav_asana", "nav_general", "nav_analysis"]),
        )
        st.markdown("**Análisis**")
        analysis_options = [t for t, _ in groups["Análisis"]]
        analysis_selected = st.session_state.get("nav_selected")
        if st.session_state.get("nav_group") != "nav_analysis" or analysis_selected not in analysis_options:
            analysis_index = None
        else:
            analysis_index = analysis_options.index(analysis_selected)
        analysis_choice = st.radio(
            "Análisis",
            analysis_options,
            label_visibility="collapsed",
            key="nav_analysis",
            index=analysis_index,
            on_change=_set_nav_selected,
            args=("nav_analysis", ["nav_asana", "nav_general", "nav_clockify"]),
        )
        render_sidebar_footer()

    # Resolve selected page from session state
    selected_title = st.session_state.get("nav_selected")
    selected_group_key = st.session_state.get("nav_group", "nav_asana")
    group_name = (
        "Asana"
        if selected_group_key == "nav_asana"
        else "General"
        if selected_group_key == "nav_general"
        else "Clockify"
        if selected_group_key == "nav_clockify"
        else "Análisis"
    )

    for title, fn in groups[group_name]:
        if title == selected_title:
            fn()
            break
    else:
        # fallback to Asana dashboard if selection is invalid
        st.session_state["nav_group"] = "nav_asana"
        st.session_state["nav_selected"] = groups["Asana"][0][0]
        groups["Asana"][0][1]()


if __name__ == "__main__":
    main()
