import dash_design_kit as ddk
from dash import dcc

biology_filter = ddk.ControlCard(
    children=[
        ddk.CardHeader(title="Occurrences"),
        ddk.ControlItem(
            children=[
                dcc.Checklist(
                    id="filter-bio-subtypes",
                    options=[
                        {"label": "DNA", "value": "dna"},
                        {"label": "Measurement of Fact", "value": "mof"}
                    ],
                    value=[], # default both unchecked  
                )
            ]
        )
    ]
)