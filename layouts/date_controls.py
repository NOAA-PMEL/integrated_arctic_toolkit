
import dash_design_kit as ddk
from dash import dcc, html
from datetime import date, timedelta

today = date.today()
default_start = today - timedelta(days=30)

# --- Biology date controls ---
date_controls = ddk.CollapsibleMenu(title="Date Range", default_open=False, self_collapsing=True, children=[

    # -- Store: single source of truth for biology + sst callbacks
    dcc.Store(
        id="time-range-store",
        data={
            "primary": {
                "start": default_start.isoformat(),
                "end": today.isoformat(),
            },
        }
    ),

    # -- Date Inputs ---
    html.Div(
        style={"padding": "8px 4px"},
        children=[
            html.Label(
                "Start Date",
                style={"fontSize": "11px", "marginBottom": "4px", "display": "block", "fontFamily": "inherit"}
            ),
            dcc.DatePickerSingle(
                id="date-start",
                date=default_start.isoformat(),
                display_format="YYYY-MM-DD",
                with_portal=True,
                style={"width": "100%", "marginBottom": "12px"}
            ),

            html.Label(
                "End Date",
                style={"fontSize": "11px", "marginBottom": "4px", "display": "block", "fontFamily": "inherit"}
            ),
            dcc.DatePickerSingle(
                id="date-end",
                date=default_start.isoformat(),
                display_format="YYYY-MM-DD",
                with_portal=True,
                style={"width": "100%", "marginBottom": "12px"}
            ),
            html.Button(
                "Apply",
                id="date-apply-btn",
                n_clicks=0,
                style={"width": "100%"}
            )
        ]
    )

])





# ddk.ControlCard(
#     id="biology-date-controls",
#     children=[
#         # Title inside the card
#         ddk.CardHeader(title="Biology Date Range"),

#         # Resolution toggle
#         ddk.ControlItem(
#             dcc.RadioItems(
#                 id="date-resolution",
#                 options=[
#                     {"label": " Year", "value": "year"},
#                     {"label": " Month", "value": "month"},
#                 ],
#                 value="year",
#                 inline=True
#             ),
#             label="Resolution"
#         ),

#         # Date Slider (DDK handles the padding here)
#         ddk.ControlItem(
#             dcc.RangeSlider(
#                 id="date-slider",
#                 min=2018,
#                 max=2026,
#                 step=1,
#                 value=[2018, 2026],
#                 marks={i: str(i) for i in range(2018, 2027)},
#                 tooltip={"placement": "bottom", "always_visible": True}
#             ),
#             label="Select Years"
#         ),

#         # Precise date inputs grouped in a block
#         ddk.Block([
#             ddk.ControlItem(
#                 dcc.Input(
#                     id="date-start-input",
#                     type="text",
#                     placeholder="YYYY-MM-DD",
#                     value="2018-01-01",
#                     debounce=True,
#                 ),
#                 label="From"
#             ),
#             ddk.ControlItem(
#                 dcc.Input(
#                     id="date-end-input",
#                     type="text",
#                     placeholder="YYYY-MM-DD",
#                     value="2026-12-31",
#                     debounce=True,
#                 ),
#                 label="To"
#             ),
#         ])
#     ]
# )