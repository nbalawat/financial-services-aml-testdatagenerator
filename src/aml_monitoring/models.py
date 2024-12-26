from pydantic import BaseModel, Field, field_validator, EmailStr, ConfigDict, model_validator
from typing import List, Optional, Dict, Union, Any
from datetime import date
from enum import Enum
import json
from datetime import datetime
from uuid import UUID, uuid4

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

class Entity(BaseModel):
    """Entity model for both institutions and subsidiaries."""
    entity_id: UUID
    entity_type: str  # 'institution' or 'subsidiary'
    parent_entity_id: Optional[UUID]  # NULL for institutions, parent_institution_id for subsidiaries
    created_at: datetime
    updated_at: datetime
    deleted_at: Optional[datetime]

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
    
    subsidiary_id: str = Field(default_factory=lambda: str(uuid4()))
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
    material_subsidiary: bool = Field(default=False)  # Explicitly set type and default
    risk_classification: str
    regulatory_status: str
    local_licenses: List[str]
    integration_status: str
    revenue: float
    assets: float
    liabilities: float
    financial_metrics: Dict[str, float]
    reporting_frequency: str
    requires_local_audit: bool
    corporate_governance_model: str
    is_regulated: bool
    is_customer: bool
    customer_id: Optional[str] = None
    customer_onboarding_date: Optional[str] = None
    customer_risk_rating: Optional[str] = None
    customer_status: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now().replace(microsecond=0))
    updated_at: datetime = Field(default_factory=lambda: datetime.now().replace(microsecond=0))
    deleted_at: Optional[datetime] = None

    @field_validator('acquisition_date')
    @classmethod
    def acquisition_after_incorporation(cls, v: str, info) -> str:
        if 'incorporation_date' in info.data and v < info.data['incorporation_date']:
            raise ValueError('acquisition_date must be after incorporation_date')
        return v

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
    latitude: float
    longitude: float
    timezone: str
    
    @field_validator('effective_to')
    @classmethod
    def effective_to_after_from(cls, v: Optional[str], info) -> Optional[str]:
        if v is None:
            return v
        if 'effective_from' in info.data:
            from_date = datetime.strptime(info.data['effective_from'], '%Y-%m-%d').date()
            to_date = datetime.strptime(v, '%Y-%m-%d').date()
            if to_date < from_date:
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
    nationality: str

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
    debit_account_id: str
    credit_account_id: str
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
