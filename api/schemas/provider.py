"""Pydantic request/response schemas for the provider API."""

from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, Field


class ProviderSummary(BaseModel):
    provider_npi:         str
    provider_name:        Optional[str]
    provider_type:        Optional[str]
    provider_state:       Optional[str]
    provider_city:        Optional[str]
    provider_zip:         Optional[str]
    total_procedures:     Optional[int]
    total_services:       Optional[int]
    avg_medicare_payment: Optional[Decimal]
    specialty_rank:       Optional[int]
    state_rank:           Optional[int]

    model_config = {"from_attributes": True}


class ProviderDetail(ProviderSummary):
    provider_first_name:    Optional[str] = None
    provider_gender:        Optional[str] = None
    medicare_participation: Optional[str] = None
    total_beneficiaries:    Optional[int] = None
    total_medicare_payment: Optional[Decimal] = None
    avg_submitted_charge:   Optional[Decimal] = None
    unique_hcpcs_codes:     Optional[int] = None
    dataset_year:           Optional[int] = None


class ProcedureCostSummary(BaseModel):
    hcpcs_code:              str
    hcpcs_description:       Optional[str]
    provider_state:          str
    provider_count:          Optional[int]
    avg_medicare_payment:    Optional[Decimal]
    median_medicare_payment: Optional[Decimal]
    min_medicare_payment:    Optional[Decimal]
    max_medicare_payment:    Optional[Decimal]

    model_config = {"from_attributes": True}


class GeographyCostSummary(BaseModel):
    provider_state:       str
    provider_zip:         str
    total_providers:      Optional[int]
    avg_medicare_payment: Optional[Decimal]
    avg_submitted_charge: Optional[Decimal]

    model_config = {"from_attributes": True}


class HospitalSummary(BaseModel):
    facility_id:          str
    facility_name:        Optional[str]
    city:                 Optional[str]
    state:                Optional[str]
    zip_code:             Optional[str]
    hospital_type:        Optional[str]
    hospital_ownership:   Optional[str]
    emergency_services:   Optional[bool]
    overall_rating:       Optional[int]
    readmission_national: Optional[str]
    mortality_national:   Optional[str]
    safety_national:      Optional[str]
    patient_experience:   Optional[str]
    state_rank:           Optional[int]
    national_rank:        Optional[int]

    model_config = {"from_attributes": True}


class PaginatedResponse(BaseModel):
    total: int
    page: int
    page_size: int
    results: list


class HealthResponse(BaseModel):
    status: str
    database: str
    dataset_year: Optional[int]
    total_providers: Optional[int]
