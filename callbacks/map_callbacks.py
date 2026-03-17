from dash import Input, Output, callback
import plotly.express as px
import pandas as pd
from constants import postgres_engine

@callback(
    Output("main-map", "figure"),
    Input("main-map", "id") # main figure fires on page load
)
def update_map(_):
    # Pull a sample of points - will want a representative points for main map (will do a different query upon zoom to show more detailed points)
    # This query will average lat/lon/species count into averages for each 1 degree grid in the world - will at most return 64,800 cells(180*360)
    # double quotes says column names not aliases
    query = """
        SELECT
            AVG("decimalLongitude") as longitude,
            AVG("decimalLatitude") as latitude, 
            COUNT(*) as point_count,
            COUNT(DISTINCT species) as species_count,
            'biology' as datatype
        FROM occurrence
        WHERE "decimalLongitude" IS NOT NULL
            AND "decimalLatitude" IS NOT NULL
        GROUP BY
            ROUND("decimalLongitude"::numeric, 1),
            ROUND("decimalLatitude"::numeric, 1)
        """

    df = pd.read_sql(query, postgres_engine)

    df["hover_text"] = (
        "Occurrences: " + df["point_count"].astype(str) + "<br>" +
        "Unique species: " + df["species_count"].astype(str)
    )

    fig = px.scatter_map(
        df,
        lat="latitude",
        lon="longitude",
        hover_name="datatype",
        custom_data=["hover_text"],
        zoom=2,
        height=700,
        map_style="open-street-map"
    )

    fig.update_traces(
        hovertemplate="<b>%{hovertext}</b><br>%{customdata[0]}<extra></extra>"
    )

    return fig