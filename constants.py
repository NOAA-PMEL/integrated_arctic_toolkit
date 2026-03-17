import os
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

connection_string = os.environ['SOUR_DATABASE_URL'].replace(
    "postgresql://", "postgresql+psycopg2://"
)

postgres_engine = create_engine(connection_string, poolclass=NullPool,)
