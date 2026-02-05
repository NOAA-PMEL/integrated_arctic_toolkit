from arctic_postgres.database import SessionLocal, engine
from arctic_postgres.models import create_tables, mof, dna_derived, occurrence

def load_data():
    create_tables()

    session = SessionLocal()