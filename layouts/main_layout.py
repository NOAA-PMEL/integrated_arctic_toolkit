from dash import dcc, html

layout = html.Div([
    html.H1("Integrated Arctic Toolkit"),
    html.Div([
        dcc.Checklist(
            id="layer-toggle",
            options=[
                {"label": " Biology occurrences", "value": "biology"},
                {"label": " SST (Sea Surface Temperature)", "value": "sst"},
            ],
            value=["biology"], # biology on by default, SST off
            inline=True
        )
    ]),
    dcc.Graph(id="main-map") # callback will fill this
    ])