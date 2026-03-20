import dash_design_kit as ddk
from dash import dcc, html
from constants import theme
from layouts import date_controls, map_layers, biology_filter

# TODO: Fix font of dcc Items (e.g. "Biology" option)

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
        ddk.Sidebar(
            id="sidebar",
            className="wide-sidebar",
            foldable=False,     # This prevents it from "folding" into icons on smaller screens
            children=[
                ddk.Menu(children=[
                    map_layers,
                    # --- FILTERS ---
                    ddk.CollapsibleMenu(
                        title="Filters",
                        default_open=True,
                        children=[biology_filter,
                            date_controls,
                        ]
                    )
                ]
            ),
        ]
    ),

    ddk.SidebarCompanion([
            ddk.Graph(id="main-map",)
            ]
        )
    ]
)


#                 ddk.Sidebar(
#                     id="sidebar",
#                     children=[
#                         ddk.Menu(
#                             children=[
                 
#                                 map_layers,
#                                 # --- FILTERS ---
#                                 ddk.CollapsibleMenu(
#                                     title="Filters",
#                                     default_open=True,
#                                     children=[biology_filter,
#                                         date_controls,
#                                     ]
#                                 )
#                             ]
#                         ),
#                     ]
#                 ),

#                 ddk.SidebarCompanion([
#                       ddk.Graph(id="main-map")
#                 ])

#             ]
#         )
#     ]
# )


# layout = ddk.App(
#     theme=theme,
#     show_editor=True,
#     children=[
#         ddk.Header(
#             children=[
#                 ddk.Logo(src="assets/noaa-logo-rgb-2022.png"),
#                 ddk.Title("Integrated Arctic Toolkit"),
#             ],
#         ),
#         # Using ddk.Row to wrap the Sidebar and the Map together
#         ddk.Row(children=[
#             # --- SIDEBAR (Now 30% width) ---
#             ddk.Block(id='sidebar', width=20, children=[
#                 ddk.ControlCard(
#                     children=[
#                         map_layers
#                     ]
#                 )
#                 # You can add biology_filter and date_controls here too
#             ]),

#             # --- MAIN CONTENT (70% width) ---
#             ddk.Block(id='map-container', width=80, children=[
#                 ddk.Graph(id="main-map")
#             ]),
#         ]) 
#     ] # End of App children
# ) # End of App