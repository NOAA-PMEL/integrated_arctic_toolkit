from dash import dcc, html

# --- Biology date controls (only show when biology is toggled on) ---
date_controls = html.Div(
    id="biology-date-controls", 
    children=[
        html.Label("Biology Date Range"),

        # Resolution toggle
        dcc.RadioItems(
            id="date-resolution",
            options=[
                {"label": " Year", "value": "year"},
                {"label": " Month", "value": "month"},
            ],
            value="year",
            inline=True
        ),

        # Date Slider
        dcc.RangeSlider(
            id="date-slider",
            min=2018,
            max=2026,
            step=1,
            value=[2018, 2026],
            marks={i: str(i) for i in range(2018, 2027)},
            tooltip={"placement": "bottom", "always_visible": True}
        ),

        # Precise date inputs below slider
        html.Div([
            html.Div([
                html.Label("From"),
                dcc.Input(
                    id="date-start-input",
                    type="text",
                    placeholder="YYYY-MM-DD",
                    value="2018-01-01",
                    debounce=True,
                    style={"width": "120px"}
                )
            ], style={"display": "inline-block", "marginRight": "40px"}),

            html.Div([
                html.Label("To"),
                # FIXED: Removed the extra ) that was after dcc.Input
                dcc.Input(
                    id="date-end-input",
                    type="text",
                    placeholder="YYYY-MM-DD",
                    value="2026-12-31",
                    debounce=True,
                    style={"width": "120px"}
                )
            ], style={"display": "inline-block"})
        ], style={"marginTop": "10px"})
    ]
)