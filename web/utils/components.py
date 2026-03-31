from __future__ import annotations

from datetime import datetime

import pandas as pd
import streamlit as st

from utils.palette import apply_plotly_theme


def apply_theme() -> None:
    """Inject shared visual style for all Streamlit report pages."""
    apply_plotly_theme()
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

        :root {
            --bg-canvas: #f8fafc;
            --surface: #ffffff;
            --surface-alt: #f1f5f9;
            --text-main: #000000;
            --text-muted: #374151;
            --text-soft: #4b5563;
            --accent: #3b82f6;
            --accent-dark: #2563eb;
            --accent-soft: rgba(59, 130, 246, 0.08);
            --success: #22c55e;
            --danger: #ef4444;
            --warning: #f59e0b;
            --border: #e2e8f0;
            --border-light: #f1f5f9;
            --shadow-sm: 0 1px 2px rgba(0,0,0,0.04);
            --shadow-md: 0 2px 8px rgba(0,0,0,0.06);
            --shadow-lg: 0 4px 16px rgba(0,0,0,0.08);
            --radius: 12px;
            --radius-lg: 16px;
            --transition: 0.2s ease;
        }

        html, body, [class*="css"] {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
        }

        code, pre, .stCode {
            font-family: 'JetBrains Mono', monospace !important;
        }

        .stApp {
            background: var(--bg-canvas);
            color: var(--text-main);
        }

        [data-testid="stHeader"] {
            background: rgba(248, 250, 252, 0.85) !important;
            backdrop-filter: blur(12px);
            -webkit-backdrop-filter: blur(12px);
            border-bottom: 1px solid var(--border);
        }

        .block-container {
            padding-top: 2.5rem;
            padding-bottom: 3rem;
            max-width: 1400px;
        }

        /* ===== SIDEBAR ===== */
        [data-testid="stSidebar"] {
            background: var(--surface) !important;
            border-right: 1px solid var(--border) !important;
        }

        [data-testid="stSidebar"] .block-container {
            padding-top: 1.25rem;
        }

        [data-testid="stSidebar"] h1,
        [data-testid="stSidebar"] h2,
        [data-testid="stSidebar"] h3,
        [data-testid="stSidebar"] label,
        [data-testid="stSidebar"] p,
        [data-testid="stSidebar"] span {
            color: var(--text-main) !important;
        }

        /* ===== TYPOGRAPHY ===== */
        h1, h2, h3 {
            color: var(--text-main);
            font-weight: 700;
            letter-spacing: -0.025em;
        }

        h1 { font-size: 1.85rem !important; margin-bottom: 0.25rem !important; }
        h2 { font-size: 1.45rem !important; }
        h3 { font-size: 1.15rem !important; margin-top: 0 !important; margin-bottom: 0 !important; }

        p, li, .stCaptionContainer { color: var(--text-muted); }

        /* ===== CARD ===== */
        .olist-card {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: var(--radius-lg);
            padding: 20px 24px;
            box-shadow: var(--shadow-sm);
            margin-bottom: 12px;
        }

        /* ===== HERO ===== */
        .olist-hero {
            background: linear-gradient(135deg, #1e293b 0%, #334155 50%, #1e3a5f 100%);
            border: none;
            border-radius: var(--radius-lg);
            padding: 36px 34px;
            margin-bottom: 24px;
            box-shadow: var(--shadow-lg);
        }

        .olist-hero::before { display: none; }

        .olist-eyebrow {
            display: inline-block;
            margin-bottom: 12px;
            color: var(--accent);
            font-size: 0.72rem;
            font-weight: 600;
            letter-spacing: 0.12em;
            text-transform: uppercase;
        }

        .olist-hero .olist-eyebrow {
            color: #93c5fd;
        }

        .olist-hero h1,
        .olist-card.olist-hero h1 {
            color: #ffffff !important;
            -webkit-text-fill-color: #ffffff !important;
            font-size: 2.2rem !important;
            font-weight: 700 !important;
            margin: 0;
            line-height: 1.2;
        }

        .olist-hero p,
        .olist-card.olist-hero p {
            color: #e2e8f0 !important;
            font-size: 0.95rem;
            line-height: 1.7;
            margin: 14px 0 0 0;
            max-width: 780px;
        }

        .olist-badge {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            background: rgba(255, 255, 255, 0.15);
            border: 1px solid rgba(255, 255, 255, 0.25);
            border-radius: 8px;
            color: #ffffff;
            font-size: 0.75rem;
            font-weight: 500;
            padding: 6px 12px;
            margin: 12px 6px 0 0;
        }

        /* ===== SECTION HEADER ===== */
        .olist-section-shell {
            margin: 28px 0 14px 0;
            padding-left: 2px;
        }

        .olist-section-title {
            display: flex;
            align-items: center;
            gap: 10px;
            color: var(--text-main);
            font-size: 1.1rem;
            font-weight: 700;
            letter-spacing: -0.015em;
        }

        .olist-section-icon {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: 32px;
            height: 32px;
            border-radius: 8px;
            background: var(--accent-soft);
            color: var(--accent);
            font-size: 0.85rem;
            font-weight: 600;
        }

        .olist-section-caption {
            margin-top: 4px;
            color: var(--text-soft);
            font-size: 0.88rem;
        }

        /* ===== KPI CARD ===== */
        .olist-kpi-title {
            font-size: 0.72rem;
            font-weight: 600;
            color: var(--text-soft);
            margin-bottom: 0.5rem;
            letter-spacing: 0.06em;
            text-transform: uppercase;
        }

        .olist-kpi-value {
            font-size: 1.35rem;
            font-weight: 700;
            color: var(--text-main);
            line-height: 1.3;
            white-space: nowrap;
        }

        .olist-kpi-caption {
            font-size: 0.82rem;
            color: var(--text-muted);
            margin-top: 0.45rem;
            font-weight: 500;
        }

        /* ===== STATES ===== */
        .olist-empty,
        .olist-error {
            border-radius: var(--radius);
            padding: 16px 20px;
            font-weight: 500;
            font-size: 0.9rem;
        }

        .olist-empty {
            background: var(--surface-alt);
            border: 1px dashed var(--border);
            color: var(--text-muted);
        }

        .olist-error {
            background: #fef2f2;
            border: 1px solid #fecaca;
            color: #991b1b;
        }

        /* ===== INFO GRID ===== */
        .olist-info-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 10px;
            margin-top: 14px;
        }

        .olist-info-cell {
            background: var(--surface-alt);
            border: 1px solid var(--border-light);
            border-radius: var(--radius);
            padding: 14px 16px;
        }

        .olist-info-key {
            font-size: 0.68rem;
            color: var(--text-soft);
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            margin-bottom: 6px;
        }

        .olist-info-value {
            font-size: 0.92rem;
            font-weight: 600;
            color: var(--text-main);
            line-height: 1.4;
        }

        /* ===== FEATURE CARD ===== */
        .olist-feature-card {
            height: auto;
        }

        .olist-feature-tag {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            min-width: 40px;
            padding: 6px 12px;
            border-radius: 8px;
            background: var(--accent-soft);
            color: var(--accent-dark);
            font-size: 0.75rem;
            font-weight: 700;
            letter-spacing: 0.04em;
        }

        .olist-feature-title {
            margin-top: 14px;
            font-size: 1.02rem;
            font-weight: 700;
            color: var(--text-main);
            line-height: 1.4;
        }

        .olist-feature-description {
            margin-top: 8px;
            color: var(--text-muted);
            font-size: 0.88rem;
            line-height: 1.65;
        }

        .olist-feature-footer {
            margin-top: 14px;
            color: var(--text-soft);
            font-size: 0.78rem;
            font-weight: 500;
        }

        /* ===== LIST ===== */
        .olist-list {
            margin: 12px 0 0 0;
            padding-left: 18px;
            color: var(--text-muted);
            line-height: 1.75;
            list-style: disc;
        }

        .olist-list li + li {
            margin-top: 4px;
        }

        /* ===== METRICS ===== */
        [data-testid="stMetricValue"] {
            color: #000000 !important;
        }

        [data-testid="stMetricLabel"] {
            color: #000000 !important;
        }

        /* ===== GLOBAL TEXT COLOR ===== */
        p, li, span, label, .stCaptionContainer, .stMarkdown {
            color: #1e293b;
        }

        .olist-kpi-title {
            color: #000000;
        }

        .olist-kpi-value {
            color: #000000;
        }

        .olist-kpi-caption {
            color: #374151;
        }

        /* ===== BUTTONS ===== */
        .stButton > button,
        .stDownloadButton > button {
            background-color: #eff6ff !important;
            border: 1px solid #bfdbfe !important;
            color: #1e40af !important;
            font-weight: 600 !important;
            border-radius: 8px !important;
        }

        .stButton > button:hover,
        .stDownloadButton > button:hover {
            background-color: #dbeafe !important;
            border-color: #93c5fd !important;
        }

        /* ===== MULTISELECT TAGS ===== */
        span[data-baseweb="tag"] {
            background-color: #eff6ff !important;
            border: 1px solid #bfdbfe !important;
            color: #1e40af !important;
            border-radius: 6px !important;
        }

        span[data-baseweb="tag"] span {
            color: #1e40af !important;
        }

        span[data-baseweb="tag"] svg {
            fill: #60a5fa !important;
        }

        /* ===== SPACING FIX for Streamlit columns gap ===== */
        [data-testid="column"] > div {
            padding-bottom: 8px;
        }

        /* ===== RESPONSIVE ===== */
        @media (max-width: 768px) {
            .block-container { padding: 1rem 0.85rem 2rem 0.85rem; }
            .olist-card, .olist-hero { padding: 16px 16px; }
            .olist-hero h1, h1 { font-size: 1.5rem !important; }
            .olist-kpi-value { font-size: 1.35rem; }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def section_header(title: str, caption: str | None = None, icon: str = "") -> None:
    """Display a consistent section header with optional caption and icon."""
    icon_html = f"<span class='olist-section-icon'>{icon}</span>" if icon else ""
    caption_html = (
        f"<div class='olist-section-caption'>{caption}</div>" if caption else ""
    )
    st.markdown(
        (
            "<div class='olist-section-shell'>"
            f"<div class='olist-section-title'>{icon_html}<span>{title}</span></div>"
            f"{caption_html}"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def kpi_card(
    title: str, value: str, caption: str | None = None, trend: str | None = None
) -> None:
    """Display a KPI card with optional trend indicator."""
    trend_html = ""
    if trend:
        trend_color = "var(--success)" if trend.startswith("+") else "var(--danger)"
        trend_html = (
            f"<div style='color: {trend_color}; font-size: 0.85rem; "
            "font-weight: 700; margin-top: 10px;'>"
            f"{trend}</div>"
        )

    caption_html = f"<div class='olist-kpi-caption'>{caption}</div>" if caption else ""

    html = (
        "<div class='olist-card'>"
        f"<div class='olist-kpi-title'>{title}</div>"
        f"<div class='olist-kpi-value'>{value}</div>"
        f"{caption_html}"
        f"{trend_html}"
        "</div>"
    )
    st.markdown(html, unsafe_allow_html=True)


def empty_state(message: str, icon: str = "ℹ️") -> None:
    """Display an empty state message."""
    st.markdown(
        f"<div class='olist-empty'>{icon} {message}</div>",
        unsafe_allow_html=True,
    )


def error_state(message: str, detail: str | None = None) -> None:
    """Display an error state message."""
    block = f"<div class='olist-error'><strong>Cảnh báo:</strong> {message}"
    if detail:
        block += f"<br/><small style='opacity: 0.85'>{detail}</small>"
    block += "</div>"
    st.markdown(block, unsafe_allow_html=True)


def model_info_card(info: dict[str, object], title: str = "Thông tin model") -> None:
    """Display a model info card."""
    cells = []
    for key, value in info.items():
        display = "-" if value is None else str(value)
        cells.append(
            "<div class='olist-info-cell'>"
            f"<div class='olist-info-key'>{key}</div>"
            f"<div class='olist-info-value'>{display}</div>"
            "</div>"
        )
    html = (
        "<div class='olist-card'>"
        f"<div class='olist-section-title'><span>{title}</span></div>"
        "<div class='olist-info-grid'>" + "".join(cells) + "</div></div>"
    )
    st.markdown(html, unsafe_allow_html=True)


def dataframe_download_button(
    df: pd.DataFrame, label: str, filename_prefix: str
) -> None:
    """Display a styled download button for dataframes."""
    if df.empty:
        st.caption("Không có dữ liệu để tải xuống.")
        return
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    st.download_button(
        label,
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=f"{filename_prefix}_{ts}.csv",
        mime="text/csv",
        use_container_width=True,
    )


def feature_card(
    title: str, description: str, icon: str, page_link: str | None = None
) -> None:
    """Display a report-style feature card."""
    footer = page_link or "Mở từ menu điều hướng bên trái"
    html = (
        "<div class='olist-card olist-feature-card'>"
        f"<div class='olist-feature-tag'>{icon}</div>"
        f"<div class='olist-feature-title'>{title}</div>"
        f"<div class='olist-feature-description'>{description}</div>"
        f"<div class='olist-feature-footer'>{footer}</div>"
        "</div>"
    )
    st.markdown(html, unsafe_allow_html=True)


def hero_section(title: str, subtitle: str, badges: list[str] | None = None) -> None:
    """Display a hero section for the home page."""
    badge_html = ""
    if badges:
        badge_items = "".join(
            [f"<span class='olist-badge'>{badge}</span>" for badge in badges]
        )
        badge_html = f"<div style='margin-top: 16px; display: flex; flex-wrap: wrap; gap: 6px;'>{badge_items}</div>"

    html = (
        "<div class='olist-card olist-hero'>"
        "<div class='olist-eyebrow'>Olist Analytics</div>"
        f"<h1>{title}</h1>"
        f"<p>{subtitle}</p>"
        f"{badge_html}"
        "</div>"
    )
    st.markdown(html, unsafe_allow_html=True)


def status_badge(status: str, is_success: bool = True) -> str:
    """Return HTML for a status badge."""
    if is_success:
        color = "#16a34a"
        bg_color = "#f0fdf4"
        border_color = "#bbf7d0"
        dot_bg = "#22c55e"
    else:
        color = "#dc2626"
        bg_color = "#fef2f2"
        border_color = "#fecaca"
        dot_bg = "#ef4444"
    dot = (
        f"<span style='width: 7px; height: 7px; border-radius: 50%; "
        f"background: {dot_bg}; display: inline-block;'></span>"
    )
    return (
        "<span style='display: inline-flex; align-items: center; gap: 7px; "
        f"background: {bg_color}; color: {color}; border: 1px solid {border_color}; "
        "padding: 6px 12px; border-radius: 8px; font-size: 0.8rem; font-weight: 600;'>"
        f"{dot}{status}</span>"
    )


def metric_row(metrics: list[dict]) -> None:
    """Display a row of metrics in a beautiful grid."""
    cols = st.columns(len(metrics))
    for idx, metric in enumerate(metrics):
        with cols[idx]:
            kpi_card(
                title=metric.get("title", ""),
                value=metric.get("value", ""),
                caption=metric.get("caption"),
                trend=metric.get("trend"),
            )
