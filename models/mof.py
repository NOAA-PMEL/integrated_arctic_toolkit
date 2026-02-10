from typing import Optional
from sqlalchemy import Integer, String, Text, PrimaryKeyConstraint, ForeignKeyConstraint
from sqlalchemy.orm import relationship, Mapped, mapped_column
from models.occurrence import Occurrence
from database import Base


class MeasurementOfFact(Base):
   __tablename__ = 'mof'

   data_source: Mapped[str] = mapped_column(String(4), primary_key=True)
   source_id: Mapped[str] = mapped_column(String(50), primary_key=True)

   occurrence_source_id: Mapped[str] = mapped_column(Text, index=True)
   
   datasetkey: Mapped[Optional[str]] = mapped_column(String(36))
   measurementID: Mapped[Optional[str]] = mapped_column(Text)
   occurrenceID: Mapped[Optional[str]] = mapped_column(Text, comment="From GBIF, the univerisal Darwin Core identifier created by the data producer. May be aligned with the id field below from Obis?")
   measurementType: Mapped[Optional[str]] = mapped_column(Text, index=True)
   measurementtypeid: Mapped[Optional[str]] = mapped_column(Text)
   measurementValue: Mapped[Optional[str]] = mapped_column(Text)
   measurementvalueid: Mapped[Optional[str]] = mapped_column(Text)
   measurementAccuracy: Mapped[Optional[str]] = mapped_column(Text)
   measurementUnit: Mapped[Optional[str]] = mapped_column(String(64), index=True)
   measurementunitid: Mapped[Optional[str]] = mapped_column(Text)
   measurementDeterminedDate: Mapped[Optional[str]] = mapped_column(String(64))
   measurementDeterminedBy: Mapped[Optional[str]] = mapped_column(Text)
   measurementMethod: Mapped[Optional[str]] = mapped_column(Text)
   measurementRemarks: Mapped[Optional[str]] = mapped_column(Text)
   _event_id: Mapped[Optional[str]] = mapped_column(Text)
   id: Mapped[Optional[str]] = mapped_column(Text, comment="From OBIS, not sure what this is, may be the universal OccurrenceID from DarwinCore")
   _id: Mapped[Optional[str]] = mapped_column(Text)
   _event_id: Mapped[Optional[str]] = mapped_column(Text)
   level: Mapped[Optional[int]] = mapped_column(Integer, comment="From OBIS, not sure.")

   __table_args__ = (
      PrimaryKeyConstraint('data_source', 'source_id', name='mof_pkey'),
      ForeignKeyConstraint(
         ['data_source', 'occurrence_source_id'],
         ['occurrence.data_source', 'occurrence.source_id'],
         name='fk_mof_occurrrence'
      ),
      )
   
   # Add relationship so we can do things like MeasurementOfFocat.occurrence for querying
   occurrence = relationship("Occurrence", foreign_keys=[data_source, occurrence_source_id])