"""
Comprehensive tests for currency calculation functions.

Tests cover:
- Day currency (14 CFR 61.57(a))
- Night currency (14 CFR 61.57(b))
- IFR currency (14 CFR 61.57(c))
- Flight review (14 CFR 61.56)
- Medical certificate (14 CFR 61.23)
- Edge cases and error handling
"""
import pytest
import sys
import os
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta, date

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from index import (
    handle_get_currency,
    calculate_day_currency,
    calculate_night_currency,
    calculate_ifr_currency,
    calculate_flight_review,
    calculate_medical_certificate,
    get_user_pilot_info
)


@pytest.fixture
def mock_cursor():
    """Mock database cursor for SQL queries."""
    cursor = MagicMock()
    cursor.fetchone = MagicMock()
    cursor.fetchall = MagicMock()
    cursor.execute = MagicMock()
    return cursor


@pytest.fixture
def mock_dynamodb_table():
    """Mock DynamoDB table."""
    table = MagicMock()
    return table


@pytest.fixture
def user_with_medical():
    """Sample user data with medical certificate."""
    return {
        'userId': 'user-123',
        'pilotInfo': {
            'medicalCertificateDate': '2024-06-15',
            'medicalCertificateClass': '2',
            'dateOfBirth': '1985-03-20'
        }
    }


@pytest.fixture
def user_without_medical():
    """Sample user data without medical certificate."""
    return {
        'userId': 'user-123',
        'pilotInfo': {
            'licenseNumber': 'PPL123456'
        }
    }


class TestDayCurrency:
    """Test day currency calculation (14 CFR 61.57(a))."""
    
    def test_current_with_recent_landings(self, mock_cursor):
        """Test current status with 3+ landings in last 90 days."""
        # Mock result: 5 landings, last landing 30 days ago
        last_landing = date.today() - timedelta(days=30)
        mock_cursor.fetchone.return_value = {
            'total_landings': 5,
            'last_landing_date': last_landing
        }
        
        result = calculate_day_currency(mock_cursor, 'user-123')
        
        assert result['name'] == 'Day Currency'
        assert result['status'] == 'CURRENT'
        assert result['daysRemaining'] == 60  # 90 - 30
        assert '5 T/O & Ldg' in result['details']
        mock_cursor.execute.assert_called_once()
    
    def test_expiring_within_15_days(self, mock_cursor):
        """Test expiring status when currency expires in <15 days."""
        # Last landing 80 days ago (10 days remaining)
        last_landing = date.today() - timedelta(days=80)
        mock_cursor.fetchone.return_value = {
            'total_landings': 3,
            'last_landing_date': last_landing
        }
        
        result = calculate_day_currency(mock_cursor, 'user-123')
        
        assert result['status'] == 'EXPIRING'
        assert result['daysRemaining'] == 10
    
    def test_expired_with_insufficient_landings(self, mock_cursor):
        """Test expired status with less than 3 landings."""
        last_landing = date.today() - timedelta(days=30)
        mock_cursor.fetchone.return_value = {
            'total_landings': 2,
            'last_landing_date': last_landing
        }
        
        result = calculate_day_currency(mock_cursor, 'user-123')
        
        assert result['status'] == 'EXPIRED'
        assert result['daysRemaining'] is None
        assert 'Only 2 landings' in result['details']
        assert 'NOT current' in result['explanation']
    
    def test_expired_no_recent_flights(self, mock_cursor):
        """Test expired status with no recent flights."""
        mock_cursor.fetchone.return_value = {
            'total_landings': 0,
            'last_landing_date': None
        }
        
        result = calculate_day_currency(mock_cursor, 'user-123')
        
        assert result['status'] == 'EXPIRED'
        assert result['daysRemaining'] is None
    
    def test_sql_query_parameters(self, mock_cursor):
        """Test SQL query uses correct parameters."""
        mock_cursor.fetchone.return_value = {
            'total_landings': 0,
            'last_landing_date': None
        }
        
        calculate_day_currency(mock_cursor, 'user-456')
        
        # Verify SQL query was called with user_id
        args, kwargs = mock_cursor.execute.call_args
        assert 'user-456' in args[1]
        assert '90 days' in args[0].lower()


class TestNightCurrency:
    """Test night currency calculation (14 CFR 61.57(b))."""
    
    def test_current_with_night_landings(self, mock_cursor):
        """Test current status with 3+ night full-stop landings."""
        last_landing = date.today() - timedelta(days=30)
        mock_cursor.fetchone.return_value = {
            'night_landings': 5,
            'full_stops': 5,
            'last_night_date': last_landing
        }
        
        result = calculate_night_currency(mock_cursor, 'user-123')
        
        assert result['name'] == 'Night Currency'
        assert result['status'] == 'CURRENT'
        assert result['daysRemaining'] == 60
        assert '5 night landings' in result['details']
    
    def test_expired_insufficient_night_landings(self, mock_cursor):
        """Test expired with insufficient night landings."""
        last_landing = date.today() - timedelta(days=30)
        mock_cursor.fetchone.return_value = {
            'night_landings': 2,
            'full_stops': 5,
            'last_night_date': last_landing
        }
        
        result = calculate_night_currency(mock_cursor, 'user-123')
        
        assert result['status'] == 'EXPIRED'
        assert 'Only 2 night' in result['details']
    
    def test_expired_insufficient_full_stops(self, mock_cursor):
        """Test expired with insufficient full-stop landings."""
        last_landing = date.today() - timedelta(days=30)
        mock_cursor.fetchone.return_value = {
            'night_landings': 5,
            'full_stops': 2,
            'last_night_date': last_landing
        }
        
        result = calculate_night_currency(mock_cursor, 'user-123')
        
        assert result['status'] == 'EXPIRED'
    
    def test_requires_both_night_and_full_stop(self, mock_cursor):
        """Test that BOTH night AND full-stop requirements must be met."""
        # 3 night landings but only 2 full stops
        mock_cursor.fetchone.return_value = {
            'night_landings': 3,
            'full_stops': 2,
            'last_night_date': date.today() - timedelta(days=30)
        }
        
        result = calculate_night_currency(mock_cursor, 'user-123')
        assert result['status'] == 'EXPIRED'
        
        # 2 night landings but 3 full stops
        mock_cursor.fetchone.return_value = {
            'night_landings': 2,
            'full_stops': 3,
            'last_night_date': date.today() - timedelta(days=30)
        }
        
        result = calculate_night_currency(mock_cursor, 'user-123')
        assert result['status'] == 'EXPIRED'


class TestIFRCurrency:
    """Test IFR currency calculation (14 CFR 61.57(c))."""
    
    def test_current_with_all_requirements(self, mock_cursor):
        """Test current status with 6 approaches + holds + tracking."""
        last_flight = date.today() - timedelta(days=60)
        mock_cursor.fetchone.return_value = {
            'total_approaches': 8,
            'has_holds': True,
            'has_tracking': True,
            'last_ifr_date': last_flight
        }
        
        result = calculate_ifr_currency(mock_cursor, 'user-123')
        
        assert result['name'] == 'IFR Currency'
        assert result['status'] == 'CURRENT'
        assert result['daysRemaining'] == 120  # 180 - 60
        assert '8 approaches' in result['details']
    
    def test_expired_insufficient_approaches(self, mock_cursor):
        """Test expired with less than 6 approaches."""
        last_flight = date.today() - timedelta(days=60)
        mock_cursor.fetchone.return_value = {
            'total_approaches': 4,
            'has_holds': True,
            'has_tracking': True,
            'last_ifr_date': last_flight
        }
        
        result = calculate_ifr_currency(mock_cursor, 'user-123')
        
        assert result['status'] == 'EXPIRED'
        assert 'only 4 approaches' in result['details'].lower()
    
    def test_expired_missing_holds(self, mock_cursor):
        """Test expired when missing holding procedures."""
        last_flight = date.today() - timedelta(days=60)
        mock_cursor.fetchone.return_value = {
            'total_approaches': 8,
            'has_holds': False,
            'has_tracking': True,
            'last_ifr_date': last_flight
        }
        
        result = calculate_ifr_currency(mock_cursor, 'user-123')
        
        assert result['status'] == 'EXPIRED'
        assert 'no holding' in result['details'].lower()
    
    def test_expired_missing_tracking(self, mock_cursor):
        """Test expired when missing tracking/intercepting."""
        last_flight = date.today() - timedelta(days=60)
        mock_cursor.fetchone.return_value = {
            'total_approaches': 8,
            'has_holds': True,
            'has_tracking': False,
            'last_ifr_date': last_flight
        }
        
        result = calculate_ifr_currency(mock_cursor, 'user-123')
        
        assert result['status'] == 'EXPIRED'
        assert 'no tracking' in result['details'].lower()
    
    def test_expired_multiple_missing_requirements(self, mock_cursor):
        """Test details show all missing requirements."""
        mock_cursor.fetchone.return_value = {
            'total_approaches': 3,
            'has_holds': False,
            'has_tracking': False,
            'last_ifr_date': date.today() - timedelta(days=60)
        }
        
        result = calculate_ifr_currency(mock_cursor, 'user-123')
        
        assert result['status'] == 'EXPIRED'
        details_lower = result['details'].lower()
        assert '3 approaches' in details_lower
        assert 'holding' in details_lower
        assert 'tracking' in details_lower
    
    def test_expiring_within_15_days(self, mock_cursor):
        """Test expiring status."""
        # Last IFR flight 170 days ago (10 days remaining)
        last_flight = date.today() - timedelta(days=170)
        mock_cursor.fetchone.return_value = {
            'total_approaches': 6,
            'has_holds': True,
            'has_tracking': True,
            'last_ifr_date': last_flight
        }
        
        result = calculate_ifr_currency(mock_cursor, 'user-123')
        
        assert result['status'] == 'EXPIRING'
        assert result['daysRemaining'] == 10


class TestFlightReview:
    """Test flight review calculation (14 CFR 61.56)."""
    
    def test_current_with_recent_review(self, mock_cursor):
        """Test current status with review in last 24 months."""
        review_date = date.today() - timedelta(days=365)  # 1 year ago
        mock_cursor.fetchone.return_value = {
            'date': review_date
        }
        
        result = calculate_flight_review(mock_cursor, 'user-123')
        
        assert result['name'] == 'Flight Review'
        assert result['status'] == 'CURRENT'
        assert result['daysRemaining'] == 365  # 730 - 365
        assert 'Last review:' in result['details']
    
    def test_expiring_within_30_days(self, mock_cursor):
        """Test expiring status when review expires soon."""
        # Review 710 days ago (20 days remaining)
        review_date = date.today() - timedelta(days=710)
        mock_cursor.fetchone.return_value = {
            'date': review_date
        }
        
        result = calculate_flight_review(mock_cursor, 'user-123')
        
        assert result['status'] == 'EXPIRING'
        assert result['daysRemaining'] == 20
    
    def test_expired_no_review_found(self, mock_cursor):
        """Test expired when no flight review in logbook."""
        mock_cursor.fetchone.return_value = None
        
        result = calculate_flight_review(mock_cursor, 'user-123')
        
        assert result['status'] == 'EXPIRED'
        assert result['daysRemaining'] is None
        assert 'No flight review found' in result['explanation']
    
    def test_expired_review_over_24_months(self, mock_cursor):
        """Test expired when review is over 24 months old."""
        # Review 800 days ago (expired)
        review_date = date.today() - timedelta(days=800)
        mock_cursor.fetchone.return_value = {
            'date': review_date
        }
        
        result = calculate_flight_review(mock_cursor, 'user-123')
        
        assert result['status'] == 'EXPIRED'
        assert result['daysRemaining'] == 0
    
    def test_sql_queries_flight_review_type(self, mock_cursor):
        """Test SQL query looks for 'Flight Review' flight type."""
        mock_cursor.fetchone.return_value = None
        
        calculate_flight_review(mock_cursor, 'user-123')
        
        args, kwargs = mock_cursor.execute.call_args
        sql_query = args[0]
        assert 'flight review' in sql_query.lower()
        assert 'signed' in sql_query.lower()
        assert 'flight_types' in sql_query.lower()


class TestMedicalCertificate:
    """Test medical certificate calculation (14 CFR 61.23)."""
    
    def test_class_2_under_40(self, user_with_medical):
        """Test Class 2 medical for pilot under 40 (60 months)."""
        # Exam 1 year ago, pilot age 38
        user_with_medical['pilotInfo']['medicalCertificateDate'] = (
            date.today() - timedelta(days=365)
        ).isoformat()
        
        result = calculate_medical_certificate(user_with_medical)
        
        assert result['name'] == 'Medical (Class 2)'
        assert result['status'] == 'CURRENT'
        # 60 months ≈ 1800 days, minus 365 = 1435 days remaining
        assert result['daysRemaining'] > 1400
    
    def test_class_2_over_40(self, user_with_medical):
        """Test Class 2 medical for pilot over 40 (24 months)."""
        # Make pilot 45 years old at exam
        dob = date.today() - timedelta(days=45*365)
        user_with_medical['pilotInfo']['dateOfBirth'] = dob.isoformat()
        user_with_medical['pilotInfo']['medicalCertificateDate'] = (
            date.today() - timedelta(days=365)
        ).isoformat()
        
        result = calculate_medical_certificate(user_with_medical)
        
        assert result['status'] == 'CURRENT'
        # 24 months ≈ 720 days, minus 365 = 355 days remaining
        assert result['daysRemaining'] > 350
        assert result['daysRemaining'] < 370
    
    def test_class_1_expiration(self, user_with_medical):
        """Test Class 1 medical expiration."""
        user_with_medical['pilotInfo']['medicalCertificateClass'] = '1'
        user_with_medical['pilotInfo']['medicalCertificateDate'] = (
            date.today() - timedelta(days=100)
        ).isoformat()
        
        result = calculate_medical_certificate(user_with_medical)
        
        assert result['name'] == 'Medical (Class 1)'
        assert result['status'] in ['CURRENT', 'EXPIRING']
    
    def test_class_3_expiration(self, user_with_medical):
        """Test Class 3 medical expiration."""
        user_with_medical['pilotInfo']['medicalCertificateClass'] = '3'
        
        result = calculate_medical_certificate(user_with_medical)
        
        assert result['name'] == 'Medical (Class 3)'
    
    def test_expiring_within_30_days(self, user_with_medical):
        """Test expiring status."""
        # Medical expires in 20 days (exam was 700 days ago, expires at 720)
        exam_date = date.today() - timedelta(days=700)
        user_with_medical['pilotInfo']['medicalCertificateDate'] = exam_date.isoformat()
        # Make pilot 45 (24 month validity)
        dob = exam_date - timedelta(days=45*365)
        user_with_medical['pilotInfo']['dateOfBirth'] = dob.isoformat()
        
        result = calculate_medical_certificate(user_with_medical)
        
        assert result['status'] == 'EXPIRING'
        assert result['daysRemaining'] < 30
        assert result['daysRemaining'] > 0
    
    def test_expired_medical(self, user_with_medical):
        """Test expired medical certificate."""
        # Medical exam 3 years ago (well expired for 24-month validity)
        exam_date = date.today() - timedelta(days=1095)
        user_with_medical['pilotInfo']['medicalCertificateDate'] = exam_date.isoformat()
        # Make pilot 45 (24 month validity)
        dob = exam_date - timedelta(days=45*365)
        user_with_medical['pilotInfo']['dateOfBirth'] = dob.isoformat()
        
        result = calculate_medical_certificate(user_with_medical)
        
        assert result['status'] == 'EXPIRED'
        assert result['daysRemaining'] == 0
    
    def test_not_applicable_no_medical_data(self, user_without_medical):
        """Test NOT_APPLICABLE when no medical certificate entered."""
        result = calculate_medical_certificate(user_without_medical)
        
        assert result['status'] == 'NOT_APPLICABLE'
        assert result['daysRemaining'] is None
        assert 'not found' in result['explanation'].lower()
    
    def test_not_applicable_missing_class(self, user_with_medical):
        """Test NOT_APPLICABLE when medical class is missing."""
        del user_with_medical['pilotInfo']['medicalCertificateClass']
        
        result = calculate_medical_certificate(user_with_medical)
        
        assert result['status'] == 'NOT_APPLICABLE'


class TestGetUserPilotInfo:
    """Test DynamoDB user info retrieval."""
    
    @patch('index.dynamodb')
    def test_get_user_with_data(self, mock_dynamodb, user_with_medical):
        """Test fetching user with pilot info."""
        mock_table = MagicMock()
        mock_dynamodb.Table.return_value = mock_table
        mock_table.get_item.return_value = {
            'Item': user_with_medical
        }
        
        result = get_user_pilot_info('user-123')
        
        assert result['userId'] == 'user-123'
        assert 'pilotInfo' in result
        mock_table.get_item.assert_called_once_with(Key={'userId': 'user-123'})
    
    @patch('index.dynamodb')
    def test_get_user_not_found(self, mock_dynamodb):
        """Test fetching non-existent user."""
        mock_table = MagicMock()
        mock_dynamodb.Table.return_value = mock_table
        mock_table.get_item.return_value = {}
        
        result = get_user_pilot_info('user-999')
        
        assert result == {}
    
    @patch('index.dynamodb')
    def test_get_user_error_handling(self, mock_dynamodb):
        """Test error handling in DynamoDB fetch."""
        mock_table = MagicMock()
        mock_dynamodb.Table.return_value = mock_table
        mock_table.get_item.side_effect = Exception('DynamoDB error')
        
        result = get_user_pilot_info('user-123')
        
        assert result == {}


class TestHandleGetCurrency:
    """Test main getCurrency handler."""
    
    @patch('index.get_db_connection')
    @patch('index.return_db_connection')
    @patch('index.get_user_pilot_info')
    def test_get_currency_all_current(self, mock_get_user, mock_return_conn, mock_get_conn, user_with_medical):
        """Test getCurrency with all currencies current."""
        # Mock database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn
        
        # Mock user data
        mock_get_user.return_value = user_with_medical
        
        # Mock cursor responses for each query
        mock_cursor.fetchone.side_effect = [
            # Day currency
            {'total_landings': 5, 'last_landing_date': date.today() - timedelta(days=30)},
            # Night currency
            {'night_landings': 5, 'full_stops': 5, 'last_night_date': date.today() - timedelta(days=30)},
            # IFR currency
            {'total_approaches': 8, 'has_holds': True, 'has_tracking': True, 'last_ifr_date': date.today() - timedelta(days=60)},
            # Flight review
            {'date': date.today() - timedelta(days=365)},
        ]
        
        result = handle_get_currency('user-123', {})
        
        assert 'dayCurrency' in result
        assert 'nightCurrency' in result
        assert 'ifrCurrency' in result
        assert 'flightReview' in result
        assert 'medicalCertificate' in result
        
        assert result['dayCurrency']['status'] == 'CURRENT'
        assert result['nightCurrency']['status'] == 'CURRENT'
        assert result['ifrCurrency']['status'] == 'CURRENT'
        assert result['flightReview']['status'] == 'CURRENT'
        
        mock_return_conn.assert_called_once_with(mock_conn)
    
    @patch('index.get_db_connection')
    @patch('index.return_db_connection')
    @patch('index.get_user_pilot_info')
    def test_get_currency_mixed_statuses(self, mock_get_user, mock_return_conn, mock_get_conn, user_with_medical):
        """Test getCurrency with mixed current/expired statuses."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn
        mock_get_user.return_value = user_with_medical
        
        # Mock: day current, night expired, IFR expired, flight review current
        mock_cursor.fetchone.side_effect = [
            {'total_landings': 5, 'last_landing_date': date.today() - timedelta(days=30)},
            {'night_landings': 1, 'full_stops': 1, 'last_night_date': date.today() - timedelta(days=100)},
            {'total_approaches': 3, 'has_holds': False, 'has_tracking': False, 'last_ifr_date': None},
            {'date': date.today() - timedelta(days=365)},
        ]
        
        result = handle_get_currency('user-123', {})
        
        assert result['dayCurrency']['status'] == 'CURRENT'
        assert result['nightCurrency']['status'] == 'EXPIRED'
        assert result['ifrCurrency']['status'] == 'EXPIRED'
        assert result['flightReview']['status'] == 'CURRENT'
    
    @patch('index.get_db_connection')
    @patch('index.return_db_connection')
    @patch('index.get_user_pilot_info')
    def test_get_currency_connection_cleanup(self, mock_get_user, mock_return_conn, mock_get_conn):
        """Test database connection is always cleaned up."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn
        mock_get_user.return_value = {}
        
        # Simulate error during currency calculation
        mock_cursor.fetchone.side_effect = Exception('Database error')
        
        with pytest.raises(Exception):
            handle_get_currency('user-123', {})
        
        # Connection should still be returned
        mock_return_conn.assert_called_once_with(mock_conn)


class TestEdgeCases:
    """Test edge cases and boundary conditions."""
    
    def test_exactly_3_landings(self, mock_cursor):
        """Test with exactly 3 landings (minimum requirement)."""
        last_landing = date.today() - timedelta(days=30)
        mock_cursor.fetchone.return_value = {
            'total_landings': 3,
            'last_landing_date': last_landing
        }
        
        result = calculate_day_currency(mock_cursor, 'user-123')
        assert result['status'] == 'CURRENT'
    
    def test_exactly_6_approaches(self, mock_cursor):
        """Test with exactly 6 approaches (minimum requirement)."""
        last_flight = date.today() - timedelta(days=60)
        mock_cursor.fetchone.return_value = {
            'total_approaches': 6,
            'has_holds': True,
            'has_tracking': True,
            'last_ifr_date': last_flight
        }
        
        result = calculate_ifr_currency(mock_cursor, 'user-123')
        assert result['status'] == 'CURRENT'
    
    def test_on_expiration_date(self, mock_cursor):
        """Test currency on the exact expiration date."""
        # Last landing exactly 90 days ago (expires today)
        last_landing = date.today() - timedelta(days=90)
        mock_cursor.fetchone.return_value = {
            'total_landings': 3,
            'last_landing_date': last_landing
        }
        
        result = calculate_day_currency(mock_cursor, 'user-123')
        # On expiration date, should be expired (0 days remaining)
        assert result['daysRemaining'] == 0
    
    def test_currency_requirements_in_output(self, mock_cursor):
        """Test that requirements are included in all outputs."""
        mock_cursor.fetchone.return_value = {
            'total_landings': 0,
            'last_landing_date': None
        }
        
        result = calculate_day_currency(mock_cursor, 'user-123')
        
        assert 'requirements' in result
        assert len(result['requirements']) > 0
        assert '14 CFR' not in result['requirements']  # Should be user-friendly
