import dash_design_kit as ddk
from dash import dcc

map_controls = ddk.ControlCard(
    children=[
        ddk.ControlItem(
            dcc.Checklist(
                id="layer-toggle",
                options=[
                    {"label": " Biology", "value": "biology"},
                    {"label": " SST", "value": "sst"},
                ],
                value=["biology"],
                inline=True
            ),
            label="Map Layers"
        )
    ]
)