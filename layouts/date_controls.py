
import dash_design_kit as ddk
from dash import dcc

# TODO: For Date Slider - what should be the min and max dates on the slider?

# --- Biology date controls ---
date_controls = ddk.ControlCard(
    id="biology-date-controls",
    children=[
        # Title inside the card
        ddk.CardHeader(title="Biology Date Range"),

        # Resolution toggle
        ddk.ControlItem(
            dcc.RadioItems(
                id="date-resolution",
                options=[
                    {"label": " Year", "value": "year"},
                    {"label": " Month", "value": "month"},
                ],
                value="year",
                inline=True
            ),
            label="Resolution"
        ),

        # Date Slider (DDK handles the padding here)
        ddk.ControlItem(
            dcc.RangeSlider(
                id="date-slider",
                min=2018,
                max=2026,
                step=1,
                value=[2018, 2026],
                marks={i: str(i) for i in range(2018, 2027)},
                tooltip={"placement": "bottom", "always_visible": True}
            ),
            label="Select Years"
        ),

        # Precise date inputs grouped in a block
        ddk.Block([
            ddk.ControlItem(
                dcc.Input(
                    id="date-start-input",
                    type="text",
                    placeholder="YYYY-MM-DD",
                    value="2018-01-01",
                    debounce=True,
                ),
                label="From"
            ),
            ddk.ControlItem(
                dcc.Input(
                    id="date-end-input",
                    type="text",
                    placeholder="YYYY-MM-DD",
                    value="2026-12-31",
                    debounce=True,
                ),
                label="To"
            ),
        ])
    ]
)