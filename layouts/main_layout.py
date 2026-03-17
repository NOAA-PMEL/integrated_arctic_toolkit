from dash import dcc, html

layout = html.Div([
    html.H1("Integrated Arctic Toolkit"),
    dcc.Graph(id="main-map")]) # callback will fill this