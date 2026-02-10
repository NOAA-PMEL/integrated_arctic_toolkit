from models.dna_derived import DnaDerived
from models.mof import MeasurementOfFact
from models.occurrence import Occurrence

from database import Base, engine

# This function creates all tables 
def create_tables():
    Base.metadata.create_all(bind=engine)