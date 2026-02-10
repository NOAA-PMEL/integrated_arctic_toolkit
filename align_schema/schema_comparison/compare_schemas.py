import pyarrow.parquet as pq
import pandas as pd
from difflib import SequenceMatcher

class SchemaComparer:
    def __init__(self, obis_parquet: str, gbif_parquet: str, output_csv_path: str):
        
        self.obis_parquet = self.read_parquet(parquet_path=obis_parquet)
        self.gbif_parquet = self.read_parquet(parquet_path=gbif_parquet)
        self.output_csv_path = output_csv_path
        self.mapping_df = self.build_csv()

    def read_parquet(self, parquet_path: str) -> pq.ParquetFile:
        return pq.ParquetFile(parquet_path)
    
    def normalize_column_name(self, col):
        return col.lower().replace('_', '').replace(' ', '')
    
    def similarity_score(self, a, b):
        """Calculate similarity between two stinrgs"""
        return SequenceMatcher(None, a, b).ratio()
    
    def build_csv(self):

        gbif_cols = set(self.gbif_parquet.schema.names)
        obis_cols = set(self.obis_parquet.schema.names)

        # Find overlap and different columns
        exact_matches = gbif_cols & obis_cols
        only_in_gbif = gbif_cols - obis_cols
        only_in_obis = obis_cols - gbif_cols

        # Find fuzzy mathces
        mapping_data = []

        # Track which columns from obis have been matches
        matched_obis = set()

        for col_gbif in only_in_gbif:
            norm_gbif = self.normalize_column_name(col_gbif)
            best_match = None
            best_score = 0

            for col_obis in only_in_obis:
                if col_obis in matched_obis:
                    continue

                norm_obis = self.normalize_column_name(col_obis)

                # Check if normalized version are identical
                if norm_gbif == norm_obis:
                    score = 1.0
                else:
                    score = self.similarity_score(norm_gbif, norm_obis)

                if score > best_score:
                    best_score = score
                    best_match = col_obis

            # Only consider it a match if score is above threshold 0.8
            if best_score >= 0.8:
                matched_obis.add(best_match)
                mapping_data.append({
                    'gbif_column': col_gbif,
                    'obis_column': best_match,
                    'normalized_name': norm_gbif,
                    'match_type': 'fuzzy',
                    'similarity': best_score,
                    'differences': f"GBIF: '{col_gbif}' vs OBIS: '{best_match}'"
                })

        # Add exact matches
        for col in exact_matches:
            mapping_data.append({
                'gbif_column': col,
                'obis_column': col,
                'normalized_name': self.normalize_column_name(col),
                'match_type': 'exact',
                'similarity': 1.0,
                'differences': f"none"
            })

        # Add unmatched columns
        unamtched_gbif = only_in_gbif - {m['gbif_column'] for m in mapping_data if m['match_type'] == 'fuzzy'}
        unmatched_obis = only_in_obis - matched_obis

        for col in unamtched_gbif: 
            mapping_data.append({
                'gbif_column': col,
                'obis_column': None,
                'normalized_name': self.normalize_column_name(col),
                'match_type': 'gbif_only',
                'similarity': None,
                'differences': f"only in GBIF"
            })

        for col in unmatched_obis:
            mapping_data.append({
                'gbif_column': None,
                'obis_column': col,
                'normalized_name': self.normalize_column_name(col),
                'match_type': 'obis_only',
                'similarity': None,
                'differences': f"only in OBIS"
            })

        # build datafraem
        mapping_df = pd.DataFrame(mapping_data)
        mapping_df = mapping_df.sort_values(['match_type', 'similarity'], ascending=[True, False])

        mapping_df.to_csv(self.output_csv_path)

        return mapping_df