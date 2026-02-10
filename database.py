import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv

# Database connection string
# Format: postgresql://username:paswword@host:port/database_name
# put in a .evn file and read in.
# Load environment variables
load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')

# Create engine (echo=True shows SQL queries for debugging)
engine = create_engine(DATABASE_URL, echo=True)

# Create session factory
SessionLocal = sessionmaker(bind=engine)

Base = declarative_base()