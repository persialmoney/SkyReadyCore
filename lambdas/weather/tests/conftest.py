"""
Pytest configuration and fixtures for TAF parsing tests.
"""
import pytest
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture
def sample_taf_data():
    """Sample TAF data structure from AWC API."""
    return {
        'airportCode': 'KJFK',
        'rawTAF': 'TAF KJFK 010000Z 0100/0124 27010KT 6SM SCT020',
        'issueTime': '2024-01-01T00:00:00Z',
        'validTimeFrom': '2024-01-01T00:00:00Z',
        'validTimeTo': '2024-01-02T00:00:00Z',
        'remarks': '',
        'forecast': []
    }


@pytest.fixture
def structured_forecast_data():
    """Sample structured forecast data."""
    return {
        'forecast': [{
            'fcstTimeFrom': '2024-01-01T00:00:00Z',
            'fcstTimeTo': '2024-01-01T06:00:00Z',
            'changeIndicator': None,
            'wdir': 270,
            'wspd': 10,
            'wspdGust': None,
            'visib': 6.0,
            'skyc1': 'SCT',
            'skyl1': 2000,
            'flightcat': 'VFR'
        }]
    }

