import dash_design_kit as ddk
from dash import dcc, html

biology_filter = ddk.ControlCard(
    style={"padding": "0px 0px", "marginBottom": "0", "marginTop": "0.5"},
    children=[
        ddk.CardHeader(title="Occurrences"),
        ddk.ControlItem(
            children=[
                dcc.Checklist(
                    id="filter-bio-subtypes",
                    options=[
                        {
                            "label": html.Span("All occurrences", title="All occurrences available."),
                            "value": "all"
                        },
                        {
                            "label": html.Span("Has DNA", title="Only records that have DNA derived metadata"), 
                            "value": "dna",
                        },
                        {
                            "label": html.Span("Has MOF", title="Only records that have Measurement of Fact metadata"), 
                            "value": "mof",
                        }
                    ],
                    value=["all"], # default both unchecked 
                    labelStyle={
                        "display": "block", 
                        "marginBottom": "0", 
                        "marginTop": "0"
                    }
                ),
            ]
        )
    ]
)