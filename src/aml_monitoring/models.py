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
    business_type: BusinessType
    incorporation_country: str
    incorporation_date: str
    onboarding_date: str
    risk_rating: RiskRating
    operational_status: OperationalStatus
    
    primary_currency: Optional[str] = None
    regulatory_status: Optional[str] = None
    primary_business_activity: Optional[str] = None
    primary_regulator: Optional[str] = None
    licenses: Optional[List[str]] = None
    aml_program_status: Optional[str] = None
    kyc_refresh_date: Optional[str] = None
    last_audit_date: Optional[str] = None
    next_audit_date: Optional[str] = None
    relationship_manager: Optional[str] = None
    relationship_status: Optional[str] = None
    swift_code: Optional[str] = None
    lei_code: Optional[str] = None
    tax_id: Optional[str] = None
    website: Optional[str] = None
    primary_contact_name: Optional[str] = None
    primary_contact_email: Optional[str] = None
    primary_contact_phone: Optional[str] = None
    annual_revenue: Optional[float] = None
    employee_count: Optional[int] = None
    year_established: Optional[int] = None
    customer_status: Optional[str] = None
    last_review_date: Optional[str] = None
    industry_codes: Optional[List[str]] = None
    public_company: bool = False
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
    industry_codes: Optional[List[str]] = None
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
    risk_rating: RiskRating
    risk_score: str
    assessment_type: str
    risk_factors: Dict[str, int]
    
    conducted_by: Optional[str] = None
    approved_by: Optional[str] = None
    findings: Optional[str] = None
    assessor: Optional[str] = None
    next_review_date: Optional[str] = None
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
    
    verification_status: Optional[str] = None
    verification_date: Optional[str] = None
    document_category: Optional[str] = None
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
    registration_date: str
    effective_from: str
    status: str
    local_registration_id: str
    
    effective_to: Optional[str] = None
    local_registration_date: Optional[str] = None
    local_registration_authority: Optional[str] = None
    notes: Optional[str] = None

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
    balance: float
    risk_rating: RiskRating
    
    last_activity_date: Optional[str] = None
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

class TransactionType(str, Enum):
    ACH = 'ach'
    WIRE = 'wire'
    CHECK = 'check'
    LOCKBOX = 'lockbox'

class TransactionStatus(str, Enum):
    COMPLETED = 'completed'
    PENDING = 'pending'
    FAILED = 'failed'
    REVERSED = 'reversed'

class Transaction(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    transaction_id: str
    transaction_type: TransactionType
    transaction_date: str
    amount: float
    currency: str
    transaction_status: TransactionStatus
    is_debit: bool
    account_id: str
    entity_id: str
    entity_type: str
    
    counterparty_account: Optional[str] = None
    counterparty_name: Optional[str] = None
    counterparty_bank: Optional[str] = None
    counterparty_entity_name: Optional[str] = None
    originating_country: Optional[str] = None
    destination_country: Optional[str] = None
    purpose: Optional[str] = None
    reference_number: Optional[str] = None
    screening_alert: bool = False
    alert_details: Optional[str] = None
    risk_score: Optional[int] = None
    processing_fee: Optional[float] = None
    exchange_rate: Optional[float] = None
    value_date: Optional[str] = None
    batch_id: Optional[str] = None
    check_number: Optional[str] = None
    wire_reference: Optional[str] = None

    @field_validator('amount')
    @classmethod
    def validate_amount(cls, v: float) -> float:
        if v <= 0:
            raise ValueError('amount must be positive')
        return v
