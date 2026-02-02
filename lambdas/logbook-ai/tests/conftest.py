"""
Pytest configuration and fixtures for logbook-ai tests.
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
    cursor.fetchone = MagicMock()
    cursor.fetchall = MagicMock()
    cursor.close = MagicMock()
    return conn, cursor


@pytest.fixture
def mock_bedrock_client():
    """Mock AWS Bedrock client."""
    client = MagicMock()
    return client


@pytest.fixture
def mock_secrets_client():
    """Mock AWS Secrets Manager client."""
    client = MagicMock()
    client.get_secret_value.return_value = {
        'SecretString': '{"username": "test_user", "password": "test_password"}'
    }
    return client


@pytest.fixture
def sample_embedding():
    """Sample embedding vector."""
    return [0.1] * 1024  # Titan embeddings are 1024-dimensional


@pytest.fixture
def sample_logbook_entries():
    """Sample logbook entries for testing."""
    return [
        {
            'entry_id': 'entry-1',
            'user_id': 'user-456',
            'date': datetime(2024, 1, 15),
            'aircraft': {'tailNumber': 'N12345', 'make': 'Cessna', 'model': '172'},
            'tail_number': 'N12345',
            'route': 'KJFK-KLGA-KJFK',
            'total_time': 1.5,
            'dual_received': 1.5,
            'remarks': 'Good flight, practiced pattern work',
            'searchable_text': 'Date: 2024-01-15. Aircraft: Cessna 172 N12345. Route: KJFK-KLGA-KJFK.',
            'created_at': datetime(2024, 1, 15, 10, 0, 0),
            'updated_at': datetime(2024, 1, 15, 10, 0, 0),
            'flight_types': ['training'],
            'route_legs': [],
            'pic': 0.0,
            'status': 'draft'
        },
        {
            'entry_id': 'entry-2',
            'user_id': 'user-456',
            'date': datetime(2024, 1, 20),
            'aircraft': {'tailNumber': 'N67890', 'make': 'Piper', 'model': 'PA-28'},
            'tail_number': 'N67890',
            'route': 'KLAX-KSAN-KLAX',
            'total_time': 2.0,
            'pic': 2.0,
            'remarks': 'Solo cross-country flight',
            'searchable_text': 'Date: 2024-01-20. Aircraft: Piper PA-28 N67890. Route: KLAX-KSAN-KLAX.',
            'created_at': datetime(2024, 1, 20, 14, 0, 0),
            'updated_at': datetime(2024, 1, 20, 14, 0, 0),
            'flight_types': ['solo', 'cross-country'],
            'route_legs': [],
            'dual_received': None,
            'status': 'signed'
        }
    ]


@pytest.fixture
def sample_search_query():
    """Sample search query."""
    return "pattern work at JFK"


@pytest.fixture
def sample_chat_query():
    """Sample chat query."""
    return "How many hours of dual instruction have I received?"


@pytest.fixture
def sample_claude_response():
    """Sample Claude API response."""
    return {
        'content': [
            {
                'type': 'text',
                'text': 'Based on your logbook, you have received 1.5 hours of dual instruction.'
            }
        ]
    }


@pytest.fixture
def sample_appsync_search_event():
    """Sample AppSync event for semantic search."""
    return {
        'identity': {
            'sub': 'user-456'
        },
        'arguments': {
            'query': 'pattern work at JFK',
            'limit': 10
        },
        'info': {
            'fieldName': 'searchLogbookEntries',
            'parentTypeName': 'Query'
        }
    }


@pytest.fixture
def sample_appsync_chat_event():
    """Sample AppSync event for chat query."""
    return {
        'identity': {
            'sub': 'user-456'
        },
        'arguments': {
            'query': 'How many hours of dual instruction have I received?'
        },
        'info': {
            'fieldName': 'chatLogbookQuery',
            'parentTypeName': 'Query'
        }
    }
