from arctic_postgres.models.dna_derived import DnaDerived
from arctic_postgres.models.mof import MeasurementOfFact
from arctic_postgres.models.occurrence import Occurrence

from arctic_postgres.database import Base, engine

# This function creates all tables 
def create_tables():
    Base.metadata.create_all(bind=engine)