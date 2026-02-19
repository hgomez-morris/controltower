import streamlit as st


def show_error(message: str, detail: str | None = None) -> None:
    if detail:
        st.error(f"{message}\n\n{detail}")
    else:
        st.error(message)
