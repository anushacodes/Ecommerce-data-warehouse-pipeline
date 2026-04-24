import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output

import sys, os

sys.path.insert(0, os.path.dirname(__file__))

import utils

# data fetching functions


def fetch_daily_sales() -> pd.DataFrame:
    sql = "SELECT * FROM daily_sales_metrics ORDER BY metric_date;"
    rows = utils.run_query(sql, fetch=True)
    return pd.DataFrame(
        rows, columns=["metric_date", "total_orders", "revenue", "avg_order_value"]
    )


def fetch_top_products(limit: int = 10) -> pd.DataFrame:
    sql = f"SELECT product_id, SUM(total_amount) as total_revenue FROM fact_orders GROUP BY product_id ORDER BY total_revenue DESC LIMIT {limit};"
    rows = utils.run_query(sql, fetch=True)
    return pd.DataFrame(rows, columns=["product_id", "total_revenue"])


# dash app layout

app = Dash(__name__)
app.title = "E-Commerce Analytics"

# color palette
COLORS = {
    "bg": "# 0f1117",
    "card": "# 1e2130",
    "accent1": "# 7b61ff",  # purple
    "accent2": "# 00d4aa",  # teal
    "accent3": "# ff6b6b",  # coral
    "accent4": "# ffd93d",  # yellow
    "text": "# e8eaf6",
    "muted": "# 8892b0",
}


def metric_card(label: str, value: str, color: str) -> html.Div:
    return html.Div(
        style={
            "background": COLORS["card"],
            "borderRadius": "12px",
            "padding": "24px 28px",
            "flex": "1",
            "borderTop": f"3px solid {color}",
            "boxShadow": "0 4px 20px rgba(0,0,0,0.3)",
        },
        children=[
            html.P(
                label,
                style={
                    "color": COLORS["muted"],
                    "fontSize": "13px",
                    "marginBottom": "8px",
                    "letterSpacing": "0.5px",
                },
            ),
            html.H2(
                value,
                style={
                    "color": color,
                    "fontSize": "32px",
                    "fontWeight": "700",
                    "margin": "0",
                },
            ),
        ],
    )


app.layout = html.Div(
    style={
        "backgroundColor": COLORS["bg"],
        "minHeight": "100vh",
        "fontFamily": "'Inter', 'Segoe UI', sans-serif",
        "padding": "32px 48px",
    },
    children=[
        # ── header ──────────────────────────────────
        html.Div(
            style={"marginBottom": "32px"},
            children=[
                html.H1(
                    "E-Commerce Analytics",
                    style={
                        "color": COLORS["text"],
                        "fontSize": "28px",
                        "fontWeight": "700",
                        "margin": "0",
                    },
                ),
                html.P(
                    "Real-time warehouse metrics powered by Redshift",
                    style={"color": COLORS["muted"], "marginTop": "4px"},
                ),
            ],
        ),
        # ── refresh button ───────────────────────────
        html.Button(
            "⟳ Refresh Data",
            id="refresh-btn",
            n_clicks=0,
            style={
                "backgroundColor": COLORS["accent1"],
                "color": COLORS["text"],
                "border": "none",
                "borderRadius": "8px",
                "padding": "10px 20px",
                "cursor": "pointer",
                "fontSize": "14px",
                "marginBottom": "28px",
            },
        ),
        # ── kpi cards ────────────────────────────────
        html.Div(
            id="kpi-cards",
            style={"display": "flex", "gap": "20px", "marginBottom": "32px"},
        ),
        # ── charts row ───────────────────────────────
        html.Div(
            style={
                "display": "grid",
                "gridTemplateColumns": "1fr 1fr",
                "gap": "24px",
                "marginBottom": "24px",
            },
            children=[
                html.Div(
                    id="revenue-chart",
                    style={
                        "background": COLORS["card"],
                        "borderRadius": "12px",
                        "padding": "20px",
                    },
                ),
                html.Div(
                    id="orders-chart",
                    style={
                        "background": COLORS["card"],
                        "borderRadius": "12px",
                        "padding": "20px",
                    },
                ),
            ],
        ),
        # ── full-width avg order value chart ─────────
        html.Div(
            style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "24px"},
            children=[
                html.Div(
                    id="aov-chart",
                    style={
                        "background": COLORS["card"],
                        "borderRadius": "12px",
                        "padding": "20px",
                    },
                ),
                html.Div(
                    id="top-products-chart",
                    style={
                        "background": COLORS["card"],
                        "borderRadius": "12px",
                        "padding": "20px",
                    },
                ),
            ],
        ),
    ],
)

# callbacks – update all charts on refresh

chart_layout = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color=COLORS["text"], family="Inter, Segoe UI, sans-serif"),
    margin=dict(l=10, r=10, t=40, b=10),
    xaxis=dict(gridcolor="# 2a2d3e", showgrid=true),
    yaxis=dict(gridcolor="# 2a2d3e", showgrid=true),
)


@app.callback(
    Output("kpi-cards", "children"),
    Output("revenue-chart", "children"),
    Output("orders-chart", "children"),
    Output("aov-chart", "children"),
    Output("top-products-chart", "children"),
    Input("refresh-btn", "n_clicks"),
)
def update_dashboard(n_clicks):
    # ── fetch data ───────────────────────────────
    df = fetch_daily_sales()
    tops = fetch_top_products(10)

    # ── kpi summary ──────────────────────────────
    total_revenue = df["revenue"].sum()
    total_orders = df["total_orders"].sum()
    avg_aov = df["avg_order_value"].mean()

    kpi_cards = [
        metric_card("Total Revenue", f"${total_revenue:,.0f}", COLORS["accent1"]),
        metric_card("Total Orders", f"{total_orders:,}", COLORS["accent2"]),
        metric_card("Avg Order Value", f"${avg_aov:,.2f}", COLORS["accent3"]),
        metric_card("Days of Data", f"{len(df)}", COLORS["accent4"]),
    ]

    # ── revenue over time ────────────────────────
    fig_rev = go.Figure()
    fig_rev.add_trace(
        go.Scatter(
            x=df["metric_date"],
            y=df["revenue"],
            mode="lines+markers",
            line=dict(color=COLORS["accent1"], width=2),
            fill="tozeroy",
            fillcolor="rgba(123,97,255,0.1)",
            name="Revenue",
        )
    )
    fig_rev.update_layout(title="Revenue Over Time", **chart_layout)

    # ── orders per day ───────────────────────────
    fig_orders = go.Figure()
    fig_orders.add_trace(
        go.Bar(
            x=df["metric_date"],
            y=df["total_orders"],
            marker_color=COLORS["accent2"],
            name="Orders",
        )
    )
    fig_orders.update_layout(title="Orders Per Day", **chart_layout)

    # ── average order value ──────────────────────
    fig_aov = go.Figure()
    fig_aov.add_trace(
        go.Scatter(
            x=df["metric_date"],
            y=df["avg_order_value"],
            mode="lines+markers",
            line=dict(color=COLORS["accent3"], width=2),
            name="AOV",
        )
    )
    fig_aov.update_layout(title="Average Order Value Over Time", **chart_layout)

    # ── top products ─────────────────────────────
    fig_top = go.Figure()
    fig_top.add_trace(
        go.Bar(
            x=tops["total_revenue"],
            y=tops["product_id"],
            orientation="h",
            marker_color=COLORS["accent4"],
            name="Revenue",
        )
    )
    fig_top.update_layout(
        title="Top 10 Products by Revenue",
        yaxis=dict(autorange="reversed", gridcolor="# 2a2d3e"),
        xaxis=dict(gridcolor="# 2a2d3e"),
        **{k: v for k, v in chart_layout.items() if k not in ("xaxis", "yaxis")},
    )

    def chart_div(fig) -> dcc.Graph:
        return dcc.Graph(figure=fig, config={"displayModeBar": False})

    return (
        kpi_cards,
        chart_div(fig_rev),
        chart_div(fig_orders),
        chart_div(fig_aov),
        chart_div(fig_top),
    )


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)
