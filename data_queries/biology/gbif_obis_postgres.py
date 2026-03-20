from app import cache
import pandas as pd
from constants import postgres_engine
from cache import cache

# TODO: need to change query for dna_derived and has_mof in occurrence table being boolean, need to update etl scripts from making 0 or 1 to accomodate ERDDAP (this has been fixed)
@cache.memoize(timeout=300) # cache biology for five minutes
def get_biology_data(bio_subtypes=None):

    filters = [
        '"decimalLongitude" IS NOT NULL',
        '"decimalLatitude" IS NOT NULL'
        ]

    if bio_subtypes:
        if "dna" in bio_subtypes:
            filters.append('"dna_derived" = 1') # TODO: Need to change this to boolean TRUE after updating etl scripts
        if "mof" in bio_subtypes:
            filters.append('"has_mof" = 1') # TODO: Need to change this to boolean TRUE after updating etl scripts

    where_clause = " AND ".join(filters)

    query = f"""
                SELECT
                    AVG("decimalLongitude") as longitude,
                    AVG("decimalLatitude") as latitude, 
                    COUNT(*) as point_count,
                    COUNT(DISTINCT species) as species_count,
                    'biology' as datatype
                FROM occurrence
                WHERE {where_clause}
                GROUP BY
                    ROUND("decimalLongitude"::numeric, 1),
                    ROUND("decimalLatitude"::numeric, 1)
                """

    df_bio = pd.read_sql(query, postgres_engine)
    return df_bio