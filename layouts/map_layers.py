import dash_design_kit as ddk
from dash import dcc, html
import dash_daq as daq
from constants import theme

map_layers = ddk.CollapsibleMenu(title="Map Layers", default_open=True, self_collapsing=True, children=[
    ddk.ControlItem(children=[
        html.Div(children=[  # Added 'children=[' here to wrap the two sections so the two switches stack on top instead of side by side
            # 2. Biology Switch Section
            html.Div(
                style={
                    "display": "flex",
                    "alignItems": "center",
                    "paddingLeft": "10px",
                    "marginBottom": "8px",
                },
                children=[
                    html.Span(
                        "Biology", 
                        title="Biological occurrence records from GBIF and OBIS.",
                        style={
                            "width": "65px",        # <--- This forces the alignment
                            "fontSize": ".85em",
                            "display": "inline-block"
                        }
                    ),
                    daq.BooleanSwitch(
                        id='biology-switch',
                        on=True,
                        color="#C5CAE9",
                        style={"display": "inline-block"}
                    ),
                ]),

            # 3. SST Switch Section
            html.Div(
                style={
                    "display": "flex",
                    "alignItems": "center",
                    "paddingLeft": "10px",
                    "marginBottom": "8px",
                },
                children=[
                    html.Span(
                        "SST", 
                        title="Sea Surface Temperature from NOAA Coral Reef Watch",
                        style={
                            "width": "65px",        # <--- Matches the width above
                            "fontSize": ".85em",
                            "display": "inline-block"
                        }
                    ),
                    daq.BooleanSwitch(
                        id='sst-switch',
                        on=False,
                        color="#C5CAE9",
                        style={"display": "inline-block"}
                    )
                ], 
            )
        ]) # End of vertical wrapper Div
    ]), # End of ControlItem
]) # End of CollapsibleMenu