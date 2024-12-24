from pydantic import BaseModel, Field, field_validator, EmailStr, ConfigDict, model_validator
from typing import List, Optional, Dict, Union, Any
from datetime import date
from enum import Enum
import json
from datetime import datetime

class BusinessType(str, Enum):
    HEDGE_FUND = 'hedge_fund'
    BANK = 'bank'
    BROKER_DEALER = 'broker_dealer'
    INSURANCE = 'insurance'
    ASSET_MANAGER = 'asset_manager'
    PENSION_FUND = 'pension_fund'
    OTHER = 'other'

class OperationalStatus(str, Enum):
    ACTIVE = 'active'
    DORMANT = 'dormant'
    LIQUIDATING = 'liquidating'

class RiskRating(str, Enum):
    LOW = 'low'
    MEDIUM = 'medium'
    HIGH = 'high'

class Institution(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    institution_id: str
    legal_name: str
    business_type: str
    incorporation_country: str
    incorporation_date: str
    onboarding_date: str
    risk_rating: str
    operational_status: str
    primary_currency: str
    regulatory_status: str
    primary_business_activity: str
    primary_regulator: str
    licenses: List[str]
    aml_program_status: str
    kyc_refresh_date: str
    last_audit_date: str
    next_audit_date: str
    relationship_manager: str
    relationship_status: str
    swift_code: str
    lei_code: str
    tax_id: str
    website: str
    primary_contact_name: str
    primary_contact_email: str
    primary_contact_phone: str
    annual_revenue: float
    employee_count: int
    year_established: int
    customer_status: str
    last_review_date: str
    industry_codes: List[str]
    public_company: bool
    stock_symbol: Optional[str] = None
    stock_exchange: Optional[str] = None

    @field_validator('onboarding_date')
    @classmethod
    def onboarding_after_incorporation(cls, v: str, info) -> str:
        if 'incorporation_date' in info.data:
            incorporation_date = datetime.strptime(info.data['incorporation_date'], '%Y-%m-%d').date()
            onboarding_date = datetime.strptime(v, '%Y-%m-%d').date()
            if onboarding_date < incorporation_date:
                raise ValueError('onboarding_date must be after incorporation_date')
        return v

class Subsidiary(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    subsidiary_id: str
    parent_institution_id: str
    legal_name: str
    tax_id: str
    incorporation_country: str
    incorporation_date: str
    acquisition_date: str
    business_type: str
    operational_status: str
    parent_ownership_percentage: float
    consolidation_status: str
    capital_investment: float
    functional_currency: str
    material_subsidiary: bool
    risk_classification: str
    regulatory_status: str
    local_licenses: List[str]
    integration_status: str
    financial_metrics: Dict[str, Union[int, float]]
    reporting_frequency: str
    requires_local_audit: bool
    corporate_governance_model: str
    is_regulated: bool
    is_customer: bool
    customer_id: Optional[str] = None
    customer_onboarding_date: Optional[str] = None
    customer_risk_rating: Optional[str] = None
    customer_status: Optional[str] = None

    @field_validator('acquisition_date')
    @classmethod
    def acquisition_after_incorporation(cls, v: str, info) -> str:
        if 'incorporation_date' in info.data and v < info.data['incorporation_date']:
            raise ValueError('acquisition_date must be after incorporation_date')
        return v

    @model_validator(mode='after')
    def validate_customer_fields(self) -> 'Subsidiary':
        if self.is_customer:
            if not all([
                self.customer_id,
                self.customer_onboarding_date,
                self.customer_risk_rating,
                self.customer_status
            ]):
                raise ValueError('All customer fields (customer_id, customer_onboarding_date, customer_risk_rating, customer_status) are required when is_customer is True')
            
            if self.customer_onboarding_date < self.incorporation_date:
                raise ValueError('customer_onboarding_date must be after incorporation_date')
        return self

class Address(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    address_id: str
    entity_id: str
    entity_type: str
    address_type: str
    address_line1: str
    address_line2: Optional[str]
    city: str
    state_province: str
    postal_code: str
    country: str
    status: str
    effective_from: str
    effective_to: Optional[str]
    primary_address: bool
    validation_status: str
    last_verified: str
    geo_coordinates: Dict[str, float]
    timezone: str

    @field_validator('effective_to')
    @classmethod
    def effective_to_after_from(cls, v: Optional[str], info) -> Optional[str]:
        if v and 'effective_from' in info.data and v < info.data['effective_from']:
            raise ValueError('effective_to must be after effective_from')
        return v

class RiskAssessment(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    assessment_id: str
    entity_id: str
    entity_type: str
    assessment_date: str
    risk_rating: str
    risk_score: str
    assessment_type: str
    risk_factors: Dict[str, int]
    conducted_by: str
    approved_by: str
    findings: str
    assessor: str
    next_review_date: str
    notes: Optional[str] = None

    @field_validator('risk_factors')
    @classmethod
    def validate_risk_scores(cls, v: Dict[str, int]) -> Dict[str, int]:
        for score in v.values():
            if not 1 <= score <= 5:
                raise ValueError('Risk scores must be between 1 and 5')
        return v

class BeneficialOwner(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    owner_id: str
    entity_id: str
    entity_type: str
    name: str
    nationality: str
    country_of_residence: str
    ownership_percentage: float
    dob: str
    verification_date: str
    pep_status: bool
    sanctions_status: bool
    adverse_media_status: bool
    verification_source: str
    notes: Optional[str] = None

class AuthorizedPerson(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    person_id: str
    entity_id: str
    entity_type: str
    name: str
    title: str
    authorization_level: str
    authorization_type: str
    authorization_start: str
    authorization_end: Optional[str] = None
    contact_info: Dict[str, str]
    is_active: bool
    last_verification_date: Optional[str] = None

    @field_validator('authorization_end')
    @classmethod
    def end_after_start(cls, v: Optional[str], info) -> Optional[str]:
        if v and 'authorization_start' in info.data and v < info.data['authorization_start']:
            raise ValueError('authorization_end must be after authorization_start')
        return v

class Document(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    document_id: str
    entity_id: str
    entity_type: str
    document_type: str
    document_number: str
    issuing_authority: str
    issuing_country: str
    issue_date: str
    expiry_date: str
    verification_status: str
    verification_date: str
    document_category: str
    notes: Optional[str] = None

class ReportingRequirements(BaseModel):
    frequency: str
    reports: List[str]
    next_due_date: str

class JurisdictionPresence(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    presence_id: str
    entity_id: str
    entity_type: str
    jurisdiction: str
    presence_type: str
    regulatory_status: str
    local_licenses: List[str]
    establishment_date: str
    local_regulator: str
    compliance_status: str
    reporting_requirements: Dict[str, Union[str, List[str]]]
    supervisory_authority: str
    material_entity_flag: bool

class Account(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    account_id: str
    entity_id: str
    entity_type: str
    account_type: str
    account_number: str
    currency: str
    status: str
    opening_date: str
    last_activity_date: str
    balance: float
    risk_rating: RiskRating
    purpose: Optional[str] = None
    average_monthly_balance: Optional[float] = None
    custodian_bank: Optional[str] = None
    account_officer: Optional[str] = None
    custodian_country: Optional[str] = None

class SubsidiaryRelationship(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    relationship_id: str
    subsidiary_id: str
    related_subsidiary_id: str
    relationship_type: str
    nature_of_business: str
    service_agreements: Dict[str, Union[str, List[str]]]
    annual_transaction_volume: float = Field(gt=0)
    transfer_pricing_model: str
    relationship_start: str
    approval_status: str
    risk_factors: Dict[str, int]

    @field_validator('risk_factors')
    @classmethod
    def validate_risk_factors(cls, v: Dict[str, int]) -> Dict[str, int]:
        for score in v.values():
            if not 1 <= score <= 5:
                raise ValueError('Risk factor scores must be between 1 and 5')
        return v

    @field_validator('related_subsidiary_id')
    @classmethod
    def different_subsidiaries(cls, v: str, info) -> str:
        if 'subsidiary_id' in info.data and v == info.data['subsidiary_id']:
            raise ValueError('related_subsidiary_id must be different from subsidiary_id')
        return v

class ComplianceEventType(str, Enum):
    ONBOARDING = 'onboarding'
    ACCOUNT_OPENED = 'account_opened'
    ACCOUNT_CLOSED = 'account_closed'
    PERIODIC_REVIEW = 'periodic_review'
    RISK_LEVEL_CHANGE = 'risk_level_change'
    ENHANCED_DUE_DILIGENCE = 'enhanced_due_diligence'
    KYC_UPDATE = 'kyc_update'
    ADVERSE_MEDIA = 'adverse_media'
    SANCTIONS_SCREENING = 'sanctions_screening'
    DOCUMENT_EXPIRY = 'document_expiry'
    OWNERSHIP_CHANGE = 'ownership_change'
    REMEDIATION = 'remediation'

class ComplianceEvent(BaseModel):
    event_id: str
    entity_id: str
    entity_type: str
    event_date: str
    event_type: str
    event_description: str
    old_state: Optional[str] = None
    new_state: str
    decision: Optional[str] = None
    decision_date: Optional[str] = None
    decision_maker: Optional[str] = None
    next_review_date: Optional[str] = None
    related_account_id: str
    notes: Optional[str] = None

class Customer(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    customer_id: str
    institution_id: str
    customer_type: str
    customer_status: str
    onboarding_date: str
    risk_rating: str
    compliance_status: str
    reporting_requirements: Dict[str, Union[str, List[str]]]
    supervisory_authority: str
    material_customer_flag: bool
