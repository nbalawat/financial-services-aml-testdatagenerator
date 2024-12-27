"""Test suite for data models."""

import pytest
from datetime import datetime
from pydantic import ValidationError

from aml_monitoring.datagenerator.models import (
    Institution, Transaction, Account, RiskAssessment,
    BusinessType, OperationalStatus, RiskRating, TransactionType,
    TransactionStatus, Customer
)

class TestInstitution:
    """Test suite for Institution model."""
    
    def test_valid_institution(self, sample_institution_data):
        """Test creating a valid institution."""
        institution = Institution(**sample_institution_data)
        assert institution.institution_id == sample_institution_data['institution_id']
        assert institution.legal_name == sample_institution_data['legal_name']
        assert institution.business_type == sample_institution_data['business_type']
        
    def test_invalid_business_type(self, sample_institution_data):
        """Test validation of invalid business type."""
        data = sample_institution_data.copy()
        data['business_type'] = 'invalid_type'  # Not a valid BusinessType enum
        with pytest.raises(ValidationError):
            Institution(**data)
            
    def test_invalid_dates(self, sample_institution_data):
        """Test validation of incorporation and onboarding dates."""
        data = sample_institution_data.copy()
        data['onboarding_date'] = '2019-12-31'  # Before incorporation
        with pytest.raises(ValidationError):
            Institution(**data)
            
    def test_optional_fields(self, sample_institution_data):
        """Test institution creation with optional fields."""
        institution = Institution(**sample_institution_data)
        assert institution.swift_code is None
        assert institution.lei_code is None
        
    def test_risk_rating_enum(self, sample_institution_data):
        """Test risk rating validation."""
        data = sample_institution_data.copy()
        data['risk_rating'] = 'invalid_rating'  # Not a valid RiskRating enum
        with pytest.raises(ValidationError):
            Institution(**data)

class TestTransaction:
    """Test suite for Transaction model."""
    
    def test_valid_transaction(self, sample_transaction_data):
        """Test creating a valid transaction."""
        transaction = Transaction(**sample_transaction_data)
        assert transaction.transaction_id == sample_transaction_data['transaction_id']
        assert transaction.amount == sample_transaction_data['amount']
        
    def test_invalid_amount(self, sample_transaction_data):
        """Test validation of transaction amount."""
        data = sample_transaction_data.copy()
        data['amount'] = -1000  # Negative amount should be invalid
        with pytest.raises(ValidationError):
            Transaction(**data)
            
    def test_transaction_type_enum(self, sample_transaction_data):
        """Test transaction type validation."""
        data = sample_transaction_data.copy()
        data['transaction_type'] = 'invalid_type'  # Not a valid TransactionType enum
        with pytest.raises(ValidationError):
            Transaction(**data)
            
    def test_optional_fields(self, sample_transaction_data):
        """Test transaction creation with optional fields."""
        transaction = Transaction(**sample_transaction_data)
        assert transaction.purpose is None
        assert transaction.batch_id is None

class TestAccount:
    """Test suite for Account model."""
    
    def test_valid_account(self, sample_account_data):
        """Test creating a valid account."""
        account = Account(**sample_account_data)
        assert account.account_id == sample_account_data['account_id']
        assert account.balance == sample_account_data['balance']
        
    def test_invalid_balance(self, sample_account_data):
        """Test validation of account balance."""
        data = sample_account_data.copy()
        data['balance'] = 'invalid_balance'  # Invalid type
        with pytest.raises(ValidationError):
            Account(**data)
            
    def test_risk_rating_enum(self, sample_account_data):
        """Test risk rating validation."""
        data = sample_account_data.copy()
        data['risk_rating'] = 'invalid_rating'  # Not a valid RiskRating enum
        with pytest.raises(ValidationError):
            Account(**data)
            
    def test_optional_fields(self, sample_account_data):
        """Test account creation with optional fields."""
        account = Account(**sample_account_data)
        assert account.purpose is None
        assert account.custodian_bank is None

class TestRiskAssessment:
    """Test suite for RiskAssessment model."""
    
    def test_valid_risk_assessment(self, sample_risk_assessment_data):
        """Test creating a valid risk assessment."""
        assessment = RiskAssessment(**sample_risk_assessment_data)
        assert assessment.assessment_id == sample_risk_assessment_data['assessment_id']
        assert assessment.risk_rating == sample_risk_assessment_data['risk_rating']
        
    def test_invalid_risk_factors(self, sample_risk_assessment_data):
        """Test validation of risk factors."""
        data = sample_risk_assessment_data.copy()
        data['risk_factors'] = {'test': 6}  # Invalid score
        with pytest.raises(ValidationError):
            RiskAssessment(**data)
            
    def test_risk_rating_enum(self, sample_risk_assessment_data):
        """Test risk rating validation."""
        data = sample_risk_assessment_data.copy()
        data['risk_rating'] = 'invalid_rating'  # Not a valid RiskRating enum
        with pytest.raises(ValidationError):
            RiskAssessment(**data)
            
    def test_optional_fields(self, sample_risk_assessment_data):
        """Test risk assessment creation with optional fields."""
        assessment = RiskAssessment(**sample_risk_assessment_data)
        assert assessment.notes is None
