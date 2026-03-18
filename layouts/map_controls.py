from dash import dcc, html

# TODO: For Date Slider - what should be the min and max dates on the slider?

map_controls = html.Div([
    # --- Layer Toggles ---
    dcc.Checklist(
        id="layer-toggle",
        options=[
            {"label": " Biology occurrences", "value": "biology"},
            {"label": " SST (Sea Surface Temperature)", "value": "sst"},
        ],
        value=["biology"], # biology on by default, SST off
        inline=True
    ),
    ])