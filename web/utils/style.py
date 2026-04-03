from __future__ import annotations

import streamlit as st

from utils.components import apply_theme


def hero(title: str, subtitle: str) -> None:
    apply_theme()
    st.markdown(
        f"""
        <div class='olist-card'>
          <h2 style='margin:0 0 6px 0;'>{title}</h2>
          <p style='margin:0; color: var(--text-muted); font-size: 0.9rem; line-height: 1.6;'>{subtitle}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )
