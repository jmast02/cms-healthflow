"""SQLAlchemy ORM models mapping to the Gold layer tables."""


from decimal import Decimal

from sqlalchemy import CHAR, Boolean, Integer, Numeric, SmallInteger, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from api.db import Base


class ProviderProfile(Base):
    __tablename__ = "provider_profiles"
    __table_args__ = {"schema": "gold"}

    provider_npi:           Mapped[str]            = mapped_column(String(20), primary_key=True)
    provider_name:          Mapped[str | None]      = mapped_column(String(255))
    provider_type:          Mapped[str | None]      = mapped_column(String(100))
    provider_state:         Mapped[str | None]      = mapped_column(CHAR(2))
    provider_city:          Mapped[str | None]      = mapped_column(String(100))
    provider_zip:           Mapped[str | None]      = mapped_column(String(10))
    provider_gender:        Mapped[str | None]      = mapped_column(CHAR(1))
    medicare_participation: Mapped[str | None]      = mapped_column(CHAR(1))
    total_procedures:       Mapped[int | None]      = mapped_column(Integer)
    total_beneficiaries:    Mapped[int | None]      = mapped_column(Integer)
    total_services:         Mapped[int | None]      = mapped_column(Integer)
    total_medicare_payment: Mapped[Decimal | None]  = mapped_column(Numeric(18, 2))
    avg_medicare_payment:   Mapped[Decimal | None]  = mapped_column(Numeric(15, 2))
    avg_submitted_charge:   Mapped[Decimal | None]  = mapped_column(Numeric(15, 2))
    unique_hcpcs_codes:     Mapped[int | None]      = mapped_column(Integer)
    specialty_rank:         Mapped[int | None]      = mapped_column(Integer)
    state_rank:             Mapped[int | None]      = mapped_column(Integer)
    dataset_year:           Mapped[int | None]      = mapped_column(SmallInteger)


class ProcedureCost(Base):
    __tablename__ = "procedure_costs"
    __table_args__ = {"schema": "gold"}

    hcpcs_code:              Mapped[str]            = mapped_column(String(10), primary_key=True)
    hcpcs_description:       Mapped[str | None]     = mapped_column(Text)
    provider_state:          Mapped[str]            = mapped_column(CHAR(2), primary_key=True)
    provider_count:          Mapped[int | None]     = mapped_column(Integer)
    total_services:          Mapped[int | None]     = mapped_column(Integer)
    avg_submitted_charge:    Mapped[Decimal | None] = mapped_column(Numeric(15, 2))
    avg_medicare_payment:    Mapped[Decimal | None] = mapped_column(Numeric(15, 2))
    median_medicare_payment: Mapped[Decimal | None] = mapped_column(Numeric(15, 2))
    min_medicare_payment:    Mapped[Decimal | None] = mapped_column(Numeric(15, 2))
    max_medicare_payment:    Mapped[Decimal | None] = mapped_column(Numeric(15, 2))
    stddev_medicare_payment: Mapped[Decimal | None] = mapped_column(Numeric(15, 2))
    dataset_year:            Mapped[int]            = mapped_column(SmallInteger, primary_key=True)


class CostByGeography(Base):
    __tablename__ = "cost_by_geography"
    __table_args__ = {"schema": "gold"}

    provider_state:       Mapped[str]            = mapped_column(CHAR(2), primary_key=True)
    provider_zip:         Mapped[str]            = mapped_column(String(10), primary_key=True)
    total_providers:      Mapped[int | None]     = mapped_column(Integer)
    total_services:       Mapped[int | None]     = mapped_column(Integer)
    avg_medicare_payment: Mapped[Decimal | None] = mapped_column(Numeric(15, 2))
    avg_submitted_charge: Mapped[Decimal | None] = mapped_column(Numeric(15, 2))
    dataset_year:         Mapped[int]            = mapped_column(SmallInteger, primary_key=True)


class HospitalRanking(Base):
    __tablename__ = "hospital_rankings"
    __table_args__ = {"schema": "gold"}

    facility_id:           Mapped[str]            = mapped_column(String(20), primary_key=True)
    facility_name:         Mapped[str | None]     = mapped_column(String(255))
    address:               Mapped[str | None]     = mapped_column(String(255))
    city:                  Mapped[str | None]     = mapped_column(String(100))
    state:                 Mapped[str | None]     = mapped_column(CHAR(2))
    zip_code:              Mapped[str | None]     = mapped_column(String(10))
    county_name:           Mapped[str | None]     = mapped_column(String(100))
    hospital_type:         Mapped[str | None]     = mapped_column(String(100))
    hospital_ownership:    Mapped[str | None]     = mapped_column(String(100))
    emergency_services:    Mapped[bool | None]    = mapped_column(Boolean)
    overall_rating:        Mapped[int | None]     = mapped_column(SmallInteger)
    readmission_national:  Mapped[str | None]     = mapped_column(String(50))
    mortality_national:    Mapped[str | None]     = mapped_column(String(50))
    safety_national:       Mapped[str | None]     = mapped_column(String(50))
    patient_experience:    Mapped[str | None]     = mapped_column(String(50))
    state_rank:            Mapped[int | None]     = mapped_column(Integer)
    national_rank:         Mapped[int | None]     = mapped_column(Integer)
    dataset_year:          Mapped[int | None]     = mapped_column(SmallInteger)
