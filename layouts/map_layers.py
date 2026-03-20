import dash_design_kit as ddk
from dash import dcc, html
import dash_daq as daq
from constants import theme

map_layers = html.Div(children=[
    # 1. The Title Section
    ddk.Row(children=[
        ddk.Title('Map Layers:', style={'fontSize': '.9em', 'paddingLeft': '5px'})
    ]),
    
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
])
    
    # # 2. The Checklist Section
    # dcc.Checklist(
    #     id="layer-toggle",
    #     options=[
    #         {
    #             "label": html.Span("Biology", title="Biological occurrence records from GBIF and OBIS."),
    #             "value": "biology"
    #         },
    #         {
    #             "label": html.Span("SST", title="Sea Surface Temperature from NOAA Coral Reef Watch"), 
    #             "value": "sst"
    #         },
    #     ],
    #     value=["biology"],
    #     # 'inherit' ensures it uses your DDK Theme font
    #     style={
    #         'fontFamily': 'inherit', 
    #         'paddingLeft': '10px',
    #         'marginTop': '5px'
    #     },
    #     # Tighten the spacing between DNA and SST
    #     labelStyle={'display': 'block', 'marginBottom': '2px'}
    # )
# ])

# map_layers = ddk.CollapsibleMenu( # collapsbileMenu can only be used inside Menu as a child
#                 title='Map Layers',
#                 default_open=True,
#                 self_collapsing=True, # will collapse when another submenu is clicked
#                 children=[
#                     ddk.ControlCard(
#                         style={"padding": "0px 0px", "marginBottom": "0", "marginTop": "0"},
#                         children=[
#                             ddk.ControlItem(
#                                 # label="Biology",
#                                 children=dcc.Checklist(
#                                     id="layer-toggle",
#                                     options=[
#                                         {
#                                             "label": html.Span("Biology", title="Biological occurrence records from GBIF and OBIS."),
#                                             "value": "biology"
#                                             },
#                                         {
#                                             "label": html.Span("SST", title="Sea Surface Temperature from NOAA Coral Reef Watch"), 
#                                             "value": "sst"},
#                                     ],
#                                     value=["biology"], # what is automatically checked
#                                 )
#                             )
#                         ]
#                     )
#                 ]
#             )