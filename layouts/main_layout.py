from dash import dcc, html
from layouts import map_controls, date_controls

layout = html.Div([
    html.H1("Integrated Arctic Toolkit"),
    map_controls,
    date_controls,

    # -- Map ---
    dcc.Graph(id="main-map") # callback will fill this
    ])