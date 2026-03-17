from app import cache
import pandas as pd
from constants import postgres_engine
from cache import cache

@cache.memoize(timeout=300) # cache biology for five minutes
def get_biology_data():
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

    df_bio = pd.read_sql(query, postgres_engine)
    return df_bio