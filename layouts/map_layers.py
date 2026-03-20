import dash_design_kit as ddk
from dash import dcc, html
import dash_daq as daq
from constants import theme

biology_switch = html.Div(
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
    ])

biology_subfilter = html.Div(
    id='bio-subfilter',
    style={
        "paddingLeft": "20px",
        "marginBottom": "10px"
    },
    children=[
        html.Div(
            "show only occurrences with:",
            style={
                "fontSize": "10px",
                "color": "gray",
                "marginBottom": "4px",
                "fontFamily": "inherit"
            },
        ),
        dcc.Checklist(
            id="filter-bio-subtypes",
            options=[
                {"label": " DNA", "value": "dna"},
                {"label": " MOF", "value": "mof"},
            ],
            value=[],
            inline=True,
            style={"fontSize": ".85em"},
            labelStyle={"marginRight": "12px"}
        )
    ]
)

sst_switch = html.Div(
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

map_layers = ddk.CollapsibleMenu(title="Map Layers", default_open=True, self_collapsing=True, children=[
    ddk.ControlItem(children=[
        html.Div(children=[  # Added 'children=[' here to wrap the two sections so the two switches stack on top instead of side by side
            biology_switch,
            biology_subfilter,
            sst_switch
        ]) 
    ]), 
])
