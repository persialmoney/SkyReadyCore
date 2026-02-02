"""
Pytest configuration and fixtures for logbook-operations tests.
"""
import pytest
import sys
import os
from unittest.mock import Mock, MagicMock
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture
def mock_db_connection():
    """Mock database connection."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    conn.commit = MagicMock()
    conn.rollback = MagicMock()
    cursor.fetchone = MagicMock()
    cursor.fetchall = MagicMock()
    cursor.close = MagicMock()
    cursor.rowcount = 1
    return conn, cursor


@pytest.fixture
def mock_bedrock_client():
    """Mock AWS Bedrock client."""
    client = MagicMock()
    response = {
        'body': MagicMock()
    }
    # Mock embedding response
    response['body'].read.return_value = '{"embedding": [0.1, 0.2, 0.3]}'.encode('utf-8')
    client.invoke_model.return_value = response
    return client


@pytest.fixture
def mock_secrets_client():
    """Mock AWS Secrets Manager client."""
    client = MagicMock()
    secret = {
        'username': 'test_user',
        'password': 'test_password'
    }
    client.get_secret_value.return_value = {
        'SecretString': '{"username": "test_user", "password": "test_password"}'
    }
    return client


@pytest.fixture
def sample_logbook_entry():
    """Sample logbook entry for testing."""
    return {
        'date': '2024-01-15',
        'aircraft': {
            'id': 'aircraft-123',
            'tailNumber': 'N12345',
            'make': 'Cessna',
            'model': '172',
            'category': 'Airplane',
            'class': 'Single Engine Land'
        },
        'tailNumber': 'N12345',
        'routeLegs': [
            {'from': 'KJFK', 'to': 'KLGA'},
            {'from': 'KLGA', 'to': 'KJFK'}
        ],
        'route': 'KJFK-KLGA-KJFK',
        'flightTypes': ['training', 'local'],
        'totalTime': 1.5,
        'pic': 0.0,
        'dualReceived': 1.5,
        'crossCountry': 0.0,
        'night': 0.0,
        'actualImc': 0.0,
        'simulatedInstrument': 0.0,
        'dayLandings': 3,
        'dayTakeoffs': 3,
        'approaches': 0,
        'holds': False,
        'tracking': False,
        'instructor': {
            'id': 'instructor-123',
            'name': 'John Smith',
            'certificateNumber': 'CFI12345'
        },
        'remarks': 'Good flight, practiced pattern work',
        'status': 'draft'
    }


@pytest.fixture
def sample_db_entry():
    """Sample database entry for testing."""
    return {
        'entry_id': 'entry-123',
        'user_id': 'user-456',
        'date': datetime(2024, 1, 15),
        'aircraft': {
            'id': 'aircraft-123',
            'tailNumber': 'N12345',
            'make': 'Cessna',
            'model': '172'
        },
        'tail_number': 'N12345',
        'route_legs': [
            {'from': 'KJFK', 'to': 'KLGA'},
            {'from': 'KLGA', 'to': 'KJFK'}
        ],
        'route': 'KJFK-KLGA-KJFK',
        'flight_types': ['training', 'local'],
        'total_time': 1.5,
        'block_out': None,
        'block_in': None,
        'pic': 0.0,
        'sic': None,
        'dual_received': 1.5,
        'dual_given': None,
        'solo': None,
        'cross_country': 0.0,
        'night': 0.0,
        'actual_imc': 0.0,
        'simulated_instrument': 0.0,
        'day_takeoffs': 3,
        'day_landings': 3,
        'night_takeoffs': None,
        'night_landings': None,
        'full_stop_landings': None,
        'approaches': 0,
        'holds': False,
        'tracking': False,
        'instructor': {
            'id': 'instructor-123',
            'name': 'John Smith'
        },
        'student': None,
        'lesson_topic': None,
        'ground_instruction': None,
        'maneuvers': [],
        'remarks': 'Good flight, practiced pattern work',
        'safety_notes': None,
        'safety_relevant': False,
        'status': 'draft',
        'signature': None,
        'created_at': datetime(2024, 1, 15, 10, 0, 0),
        'updated_at': datetime(2024, 1, 15, 10, 0, 0)
    }


@pytest.fixture
def sample_appsync_event():
    """Sample AppSync event for testing."""
    return {
        'identity': {
            'sub': 'user-456'
        },
        'arguments': {
            'input': {
                'date': '2024-01-15',
                'totalTime': 1.5,
                'dualReceived': 1.5
            }
        },
        'info': {
            'fieldName': 'createLogbookEntry',
            'parentTypeName': 'Mutation'
        }
    }
