from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# Database connection string
# Format: postgresql://username:paswword@host:port/database_name
# put in a .evn file and read in.
DATABASE_URL = "postgresql://username:paswword@host:port/database_name"

# Create engine
engine = create_engine(DATABASE_URL)

# Create session factory
SessionLocal = sessionmaker(bind=engine)

Base = declarative_base()