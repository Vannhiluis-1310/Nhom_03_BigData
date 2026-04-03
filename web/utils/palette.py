from __future__ import annotations

import plotly.graph_objects as go
import plotly.io as pio

# Clean blue analytics palette
PALETTE_HEX = [
    "#F8FAFC",
    "#EFF6FF",
    "#DBEAFE",
    "#BFDBFE",
    "#93C5FD",
    "#60A5FA",
    "#3B82F6",
    "#2563EB",
    "#1D4ED8",
    "#1E40AF",
    "#475569",
    "#1E293B",
]

PALETTE_DISCRETE = [
    "#3B82F6",
    "#10B981",
    "#F59E0B",
    "#EF4444",
    "#8B5CF6",
    "#06B6D4",
    "#EC4899",
    "#64748B",
    "#14B8A6",
    "#1E293B",
]

PALETTE_CONTINUOUS = [
    "#EFF6FF",
    "#DBEAFE",
    "#BFDBFE",
    "#93C5FD",
    "#60A5FA",
    "#3B82F6",
    "#2563EB",
    "#1D4ED8",
    "#1E40AF",
    "#1E3A8A",
]


def apply_plotly_theme() -> None:
    """Apply a professional Plotly template for analytics reporting pages."""
    template = go.layout.Template()
    template.layout = go.Layout(
        colorway=PALETTE_DISCRETE,
        paper_bgcolor="#ffffff",
        plot_bgcolor="#ffffff",
        font={"family": "Inter, -apple-system, sans-serif", "color": "#1E293B", "size": 13},
        title={"font": {"size": 15, "color": "#1E293B"}},
        legend={
            "bgcolor": "#ffffff",
            "bordercolor": "#e2e8f0",
            "borderwidth": 1,
            "font": {"size": 12},
        },
        xaxis={
            "gridcolor": "#f1f5f9",
            "linecolor": "#e2e8f0",
            "zerolinecolor": "#e2e8f0",
        },
        yaxis={
            "gridcolor": "#f1f5f9",
            "linecolor": "#e2e8f0",
            "zerolinecolor": "#e2e8f0",
        },
        margin={"t": 40, "b": 40, "l": 40, "r": 20},
    )
    template.layout.colorscale = {
        "sequential": [
            [i / (len(PALETTE_CONTINUOUS) - 1), c]
            for i, c in enumerate(PALETTE_CONTINUOUS)
        ],
        "diverging": [
            [i / (len(PALETTE_CONTINUOUS) - 1), c]
            for i, c in enumerate(PALETTE_CONTINUOUS)
        ],
    }
    pio.templates["olist_palette"] = template
    pio.templates.default = "plotly_white+olist_palette"
