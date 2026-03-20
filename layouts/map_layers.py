import dash_design_kit as ddk
from dash import dcc, html
import dash_daq as daq
from constants import theme

map_layers = html.Div(children=[
    # 1. The Title Section
    ddk.Row(children=[
        ddk.Title('Map Layers:', style={'fontSize': '.9em', 'paddingLeft': '5px'})
    ]),
    
    # Boolean switch
    # 2. Biology Switch Section
    html.Div(
        style={
            "display": "flex",
            "alignItems": "center",
            "justifyContent": "space-between",
            "paddingLeft": "10px",
            "paddingRight": "10px",
            "marginBottom": "10px",
        },
        children=[
            html.Span("Biology", style={"fontFamily": "ineherit", "fontSize": "13px"}),
            daq.BooleanSwitch(
                id='biology-switch',
                on=True,
                theme=theme,
                color="#C5CAE9", # light periwinkle
            ),
        ]),
    # 3. SST Switch Section
    html.Div([
        daq.BooleanSwitch(
            id='sst-switch',
            on=False,
            label="SST",
            theme=theme,
            labelPosition="left",
            color="#C5CAE9"
        )
    ], style={'paddingLeft': '10px', 'fontFamily': 'inherit'})
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