from dash import Input, Output, callback
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from data_queries import get_biology_data, fetch_sst

@callback(
    Output("main-map", "figure"),
    Input("layer-toggle", "value") # main figure fires on page load
)
def update_map(active_layers):
    fig = go.Figure()

    # Biology layer
    if "biology" in active_layers:
        df_bio = get_biology_data()

        fig.add_trace(go.Scattermap(
            lat=df_bio["latitude"],
            lon=df_bio["longitude"],
            mode="markers",
            marker=dict(size=6, color="blue", opacity=0.6),
            name="Biology",
            hovertemplate=(
                "Occurrences: %{customdata[0]}<br>"
                "Unique spcies: %{customdata[1]}<extra></extra>"
            ),
            customdata=df_bio[["point_count", "species_count"]].values
        ))

    # -- SST Layer --
    if "sst" in active_layers:
        df_sst = fetch_sst()

        if not df_sst.empty:
            fig.add_trace(go.Scattermap(
                lat=df_sst["latitude"],
                lon=df_sst["longitude"],
                mode="markers",
                marker=dict(
                    size=4,
                    color=df_sst["sst"],
                    colorscale="RdBu_r", # blue=cold, red=warm
                    colorbar=dict(title="SST (°C)"),
                    opacity=0.5
                ),
                name="SST",
                hovertemplate=(
                    "SST: %{customdata[0]:.1f}°C<br>"
                    "Ice fraction: %{customdata[1]:.2f}<extra></extra>"
                ),
                customdata=df_sst[["sst", "sea_ice_fraction"]].values
            )) 

    
    fig.update_layout(
        map_style="carto-positron",
        map_zoom=1,
        map_center={"lat": 20, "lon": 0},
        height=700,
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        legend=dict(
            yanchor="top", y=0.99,
            xanchor="left", x=0.01
        )
    )
    
    return fig