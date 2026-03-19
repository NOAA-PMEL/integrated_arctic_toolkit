import dash_design_kit as ddk
from dash import dcc, html
from constants import theme
from layouts import date_controls

# TODO: Fix font of dcc Items (e.g. "Biology" option)

""" Available ddk components: python -c "import dash_design_kit as ddk; print([x for x in dir(ddk) if not x.startswith('_')])"
['App', 'Block', 'Card', 'CardFooter', 'CardHeader', 'CollapsibleMenu', 'ControlCard', 'ControlItem', 'ControlsHeader', 
'DataCard', 'DataTable', 'Exclude', 'Footer', 'FullScreen', 'Graph', 'Header', 'Hero', 'Icon', 'Logo', 'Menu', 'Modal', 
'Notification', 'Page', 'PageFooter', 'PageHeader', 'Report', 'Row', 'SectionTitle', 'Sidebar', 'SidebarCompanion', 'Tag', 
'Title', 'bootstrap_css', 'datasets', 'f', 'notification_manager', 'shortcuts']
"""

layout = ddk.App(
    theme=theme,
    show_editor=True,
    children=[
        ddk.Header(
            children=[
                ddk.Logo(src="assets/noaa-logo-rgb-2022.png"),
                ddk.Title("Integrated Arctic Toolkit"),
            ],
        ),
        ddk.Row(
            children=[

                # --- SIDEBAR ----
                ddk.Sidebar(
                    id="sidebar",
                    children=[
                        ddk.Menu(
                            children=[
                                
                                # -- MAP LAYERS ----
                                ddk.CollapsibleMenu( # collapsbileMenu can only be used inside Menu as a child
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
                                ),

                                # --- FILTERS ---
                                ddk.CollapsibleMenu(
                                    title="Filters",
                                    default_open=True,
                                    children=[date_controls
                                        # ddk.ControlCard(
                                        #     children=[
                                        #         ddk.ControlItem(
                                        #             label="Date Range",
                                        #             children=dcc.DatePickerRange(
                                        #                 id='date-picker-range',
                                        #                 start_date_placeholder_text="Start",
                                        #                 end_date_placeholder_text="End",
                                        #                 display_format="YYYY-MM-DD"
                                        #             )
                                        #         )
                                        #     ]
                                        # )
                                    ]

                                )

                            ]
                        ),
                    ]
                ),

                ddk.SidebarCompanion([
                      ddk.Graph(id="main-map")
                ])
            ]
        )
    ]
)
#         ddk.Sidebar(
#             id="sidebar",
#             children=[
                
#                 # -- MAP LAYERS ---
#                 ddk.CollapsibleMenu(
#                     title="Map Layers",
#                     default_open=True,
#                     children=[
#                         html.Div("Map layers placeholders"),
#                     ],
#                 ),

#                 # -- FILTERS ---
#                 ddk.CollapsibleMenu(
#                     title="Filters",
#                     default_open=True,
#                     children=[
#                         html.Div("Filters placeholder"),
#                     ],
#                 ),

#                 # -- DONWLOAD ---
#                 ddk.CollapsibleMenu(
#                     title="Download",
#                     default_open=False,
#                     children=[
#                         html.Div("Download placeholder"),
#                     ],
#                 ),
#             ],
#         ),

#         ddk.SidebarCompanion(
#             children=[
#                 html.Div("Main content placeholder")
#             ],
#         ),
#     ],
# )
        