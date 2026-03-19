import dash_design_kit as ddk
from dash import dcc

map_layers = ddk.CollapsibleMenu( # collapsbileMenu can only be used inside Menu as a child
                title='Map Layers',
                default_open=True,
                self_collapsing=True, # will collapse when another submenu is clicked
                children=[
                    ddk.ControlCard(
                        children=[
                            ddk.ControlItem(
                                # label="Biology",
                                children=dcc.Checklist(
                                    id="layer-toggle",
                                    options=[
                                        {"label": "Biology Occurence", "value": "occurrence"},
                                        {"label": "SST", "value": "sst"},
                                    ],
                                    value=["occurrence"], # what is automatically checked
                                
                                )
                            )
                        ]
                    )
                ]
            )