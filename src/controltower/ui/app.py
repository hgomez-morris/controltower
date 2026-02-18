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
    proyectos,
    seguimiento,
)


def main():
    st.set_page_config(page_title="PMO Control Tower (MVP)", layout="wide")

    pages = {
        "Asana": [
            st.Page(dashboard.render, title="Dashboard", url_path="dashboard"),
            st.Page(proyectos.render, title="Proyectos", url_path="proyectos"),
            st.Page(findings.render, title="Findings", url_path="findings"),
            st.Page(mensajes.render, title="Mensajes", url_path="mensajes"),
            st.Page(seguimiento.render, title="Seguimiento", url_path="seguimiento"),
        ],
        "General": [
            st.Page(kpi.render, title="KPI", url_path="kpi"),
            st.Page(busqueda.render, title="Búsqueda", url_path="busqueda"),
            st.Page(plan_facturacion.render, title="Facturación", url_path="plan-facturacion"),
            st.Page(pagos.render, title="Pagos", url_path="pagos"),
        ],
        "Clockify": [
            st.Page(clockify_por_usuario.render, title="Por Usuario", url_path="clockify-por-usuario"),
            st.Page(clockify_por_proyectos.render, title="Por Proyectos", url_path="clockify-por-proyectos"),
        ],
    }

    try:
        navigator = st.navigation(pages, expanded=True)
    except TypeError:
        navigator = st.navigation(pages)
    navigator.run()


if __name__ == "__main__":
    main()
