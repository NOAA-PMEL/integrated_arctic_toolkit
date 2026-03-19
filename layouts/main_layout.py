from dash import dcc, html
from layouts import map_controls, date_controls
import dash_design_kit as ddk
from constants import theme

layout = ddk.App(
    theme=theme,
    children=[
        # 1. The Top Header
        ddk.Header(
            children=[
                ddk.Logo(src="assets/noaa-logo-rgb-2022.png"),
                ddk.Title("Integrated Arctic Toolkit"),
            ]
        ),
        ddk.Row(
            style={
                "margin": "0", 
                "padding": "0", 
                "height": "calc(100vh - 60px)",
                "alignItems": "stretch",
                "overflow": "hidden",
                "display": "flex"
                },
            children=[
                ddk.Sidebar(
                    foldable=True,
                    style={
                        "width": "400px", 
                        "minWidth": "400px",
                        "maxWidth": "400px",
                        "flexShrink": "0",
                        "height": "100%",
                        "overflowY": "auto",
                        "minHeight": "0"
                        },
                    children=[
                        ddk.ControlCard(
                            children=[
                                ddk.ControlItem(
                                    label="Map Layers",
                                    children=[
                                        dcc.Checklist(
                                            id="layer-toggle",
                                            options=[
                                                {"label": "Biology", "value": "biology"},
                                                {"label": "SST", "value": "sst"},
                                                ],
                                                value=["biology"],
                                            )
                                        ]
                                    )
                                ]
                            ),
                            ddk.ControlCard(
                                children=[
                                    ddk.CardHeader(title="Date Range"),
                                    date_controls
                                ]
                            ),
                        ]
                    ),
                ddk.Block(
                    style={
                        "flex": "1",
                        "minWidth": "0",
                        "height": "100%",
                        "padding": "0",
                        "display": "flex",
                        "flexDirection": "column",
                    },
                    children=[
                        ddk.Graph(id="main-map",style={
                            "flex": "1",
                            "height": "100%", 
                            "width": "100%",
                            "minHeight": "0"}
                        )
                    ]
                )
            ]
        )
    ]
)

        