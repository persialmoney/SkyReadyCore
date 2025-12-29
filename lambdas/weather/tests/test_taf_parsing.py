"""
Comprehensive tests for TAF parsing functionality.

Tests cover:
- Structured forecast parsing from AWC API
- AVWX raw TAF parsing
- Visibility parsing (various formats)
- Cloud condition parsing
- Time conversion
- Edge cases and error handling
"""
import pytest
import sys
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone
from typing import Dict, Any, List

# Add parent directory to path to import weather functions
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from index import (
    parse_taf_forecast,
    _parse_taf_from_raw,
    _parse_visibility_string,
    _parse_taf_sky_conditions,
    _convert_time_to_iso,
    _create_fallback_forecast
)


class TestVisibilityParsing:
    """Test visibility string parsing."""
    
    def test_parse_regular_sm(self):
        """Test parsing regular visibility like '6SM'."""
        assert _parse_visibility_string('6SM') == 6.0
        assert _parse_visibility_string('10SM') == 10.0
        assert _parse_visibility_string('1.5SM') == 1.5
    
    def test_parse_greater_than_sm(self):
        """Test parsing 'P6SM' (greater than 6 SM)."""
        assert _parse_visibility_string('P6SM') == 6.1
        assert _parse_visibility_string('P10SM') == 10.1
    
    def test_parse_fractional_sm(self):
        """Test parsing fractional visibility like '1/2SM'."""
        assert _parse_visibility_string('1/2SM') == 0.5
        assert _parse_visibility_string('3/4SM') == 0.75
        assert _parse_visibility_string('1/4SM') == 0.25
    
    def test_parse_mixed_formats(self):
        """Test parsing various visibility formats."""
        assert _parse_visibility_string('M1/4SM') is not None  # Less than 1/4 SM
        assert _parse_visibility_string('6') == 6.0
        assert _parse_visibility_string('10.5') == 10.5
    
    def test_parse_empty_or_none(self):
        """Test parsing empty or None values."""
        assert _parse_visibility_string('') is None
        assert _parse_visibility_string(None) is None
        assert _parse_visibility_string('   ') is None
    
    def test_parse_invalid_formats(self):
        """Test parsing invalid formats returns None."""
        assert _parse_visibility_string('INVALID') is None
        assert _parse_visibility_string('ABC') is None


class TestTimeConversion:
    """Test time conversion to ISO format."""
    
    def test_convert_unix_timestamp_seconds(self):
        """Test converting Unix timestamp in seconds."""
        timestamp = 1704067200  # 2024-01-01 00:00:00 UTC
        result = _convert_time_to_iso(timestamp)
        assert '2024-01-01' in result
        assert 'T' in result or 'Z' in result
    
    def test_convert_unix_timestamp_milliseconds(self):
        """Test converting Unix timestamp in milliseconds."""
        timestamp = 1704067200000  # 2024-01-01 00:00:00 UTC
        result = _convert_time_to_iso(timestamp)
        assert '2024-01-01' in result
    
    def test_convert_iso_string(self):
        """Test converting ISO string (should return as-is or normalized)."""
        iso_str = "2024-01-01T00:00:00Z"
        result = _convert_time_to_iso(iso_str)
        assert '2024-01-01' in result
    
    def test_convert_empty_string(self):
        """Test converting empty string."""
        assert _convert_time_to_iso('') == ''
        assert _convert_time_to_iso(None) == ''
    
    def test_convert_invalid_format(self):
        """Test converting invalid time format."""
        result = _convert_time_to_iso('invalid')
        assert result == 'invalid' or result == ''


class TestTAFSkyConditions:
    """Test TAF sky conditions parsing."""
    
    def test_parse_single_cloud_layer(self):
        """Test parsing single cloud layer."""
        data = {
            'skyc1': 'FEW',
            'skyl1': 1000
        }
        result = _parse_taf_sky_conditions(data)
        assert len(result) == 1
        assert result[0]['skyCover'] == 'FEW'
        assert result[0]['cloudBase'] == 1000
    
    def test_parse_multiple_cloud_layers(self):
        """Test parsing multiple cloud layers."""
        data = {
            'skyc1': 'SCT',
            'skyl1': 2000,
            'skyc2': 'BKN',
            'skyl2': 3000,
            'skyc3': 'OVC',
            'skyl3': 5000
        }
        result = _parse_taf_sky_conditions(data)
        assert len(result) == 3
        assert result[0]['skyCover'] == 'SCT'
        assert result[1]['skyCover'] == 'BKN'
        assert result[2]['skyCover'] == 'OVC'
    
    def test_parse_clear_sky(self):
        """Test parsing clear sky conditions."""
        data = {
            'skyc1': 'CLR'
        }
        result = _parse_taf_sky_conditions(data)
        assert len(result) == 1
        assert result[0]['skyCover'] == 'CLR'
        assert result[0]['cloudBase'] is None
    
    def test_parse_empty_clouds(self):
        """Test parsing when no cloud data."""
        data = {}
        result = _parse_taf_sky_conditions(data)
        assert result == []
    
    def test_parse_cloud_with_type(self):
        """Test parsing cloud with type (CB, TCU)."""
        data = {
            'skyc1': 'BKN',
            'skyl1': 2000,
            'skyt1': 'CB'
        }
        result = _parse_taf_sky_conditions(data)
        assert len(result) == 1
        assert result[0]['skyCover'] == 'BKN'
        assert result[0]['cloudType'] == 'CB'


class TestFallbackForecast:
    """Test fallback forecast creation."""
    
    def test_create_fallback_with_all_data(self):
        """Test creating fallback forecast with all data."""
        taf_data = {
            'validTimeFrom': '2024-01-01T00:00:00Z',
            'validTimeTo': '2024-01-02T00:00:00Z',
            'wdir': 270,
            'wspd': 10,
            'wspdGust': 15,
            'visib': 6.0,
            'skyc1': 'SCT',
            'skyl1': 2000,
            'flightcat': 'VFR'
        }
        result = _create_fallback_forecast(taf_data)
        assert len(result) == 1
        assert result[0]['windDirection'] == 270
        assert result[0]['windSpeed'] == 10
        assert result[0]['windGust'] == 15
        assert result[0]['visibility'] == 6.0
        assert len(result[0]['skyConditions']) == 1
    
    def test_create_fallback_with_minimal_data(self):
        """Test creating fallback forecast with minimal data."""
        taf_data = {
            'validTimeFrom': '2024-01-01T00:00:00Z',
            'validTimeTo': '2024-01-02T00:00:00Z'
        }
        result = _create_fallback_forecast(taf_data)
        assert len(result) == 1
        assert result[0]['windDirection'] is None
        assert result[0]['visibility'] is None


class TestStructuredForecastParsing:
    """Test parsing structured forecast from AWC API."""
    
    def test_parse_simple_forecast(self):
        """Test parsing simple forecast period."""
        taf = {
            'forecast': [{
                'fcstTimeFrom': '2024-01-01T00:00:00Z',
                'fcstTimeTo': '2024-01-01T06:00:00Z',
                'wdir': 270,
                'wspd': 10,
                'visib': 6.0,
                'skyc1': 'SCT',
                'skyl1': 2000
            }]
        }
        result = parse_taf_forecast(taf)
        assert len(result) == 1
        assert result[0]['windDirection'] == 270
        assert result[0]['windSpeed'] == 10
        assert result[0]['visibility'] == 6.0
    
    def test_parse_multiple_forecast_periods(self):
        """Test parsing multiple forecast periods."""
        taf = {
            'forecast': [
                {
                    'fcstTimeFrom': '2024-01-01T00:00:00Z',
                    'fcstTimeTo': '2024-01-01T06:00:00Z',
                    'wdir': 270,
                    'wspd': 10
                },
                {
                    'fcstTimeFrom': '2024-01-01T06:00:00Z',
                    'fcstTimeTo': '2024-01-01T12:00:00Z',
                    'wdir': 180,
                    'wspd': 15
                }
            ]
        }
        result = parse_taf_forecast(taf)
        assert len(result) == 2
        assert result[0]['windDirection'] == 270
        assert result[1]['windDirection'] == 180
    
    def test_parse_forecast_with_change_indicators(self):
        """Test parsing forecast with change indicators (FM, TEMPO, PROB)."""
        taf = {
            'forecast': [
                {
                    'fcstTimeFrom': '2024-01-01T00:00:00Z',
                    'fcstTimeTo': '2024-01-01T06:00:00Z',
                    'changeIndicator': 'FM',
                    'wdir': 270,
                    'wspd': 10
                },
                {
                    'fcstTimeFrom': '2024-01-01T06:00:00Z',
                    'fcstTimeTo': '2024-01-01T12:00:00Z',
                    'changeIndicator': 'TEMPO',
                    'wdir': 180,
                    'wspd': 20
                }
            ]
        }
        result = parse_taf_forecast(taf)
        assert len(result) == 2
        assert result[0]['changeIndicator'] == 'FM'
        assert result[1]['changeIndicator'] == 'TEMPO'
    
    def test_parse_forecast_with_alternative_field_names(self):
        """Test parsing forecast with alternative field names."""
        taf = {
            'forecast': [{
                'timeFrom': '2024-01-01T00:00:00Z',
                'timeTo': '2024-01-01T06:00:00Z',
                'windDirection': 270,
                'windSpeed': 10,
                'visibility': 6.0
            }]
        }
        result = parse_taf_forecast(taf)
        assert len(result) == 1
        assert result[0]['windDirection'] == 270


class TestAVWXRawParsing:
    """Test parsing raw TAF using AVWX library."""
    
    @patch('index.Taf')
    def test_parse_simple_raw_taf(self, mock_taf_class):
        """Test parsing simple raw TAF."""
        # Mock AVWX Taf object
        mock_line_data = Mock()
        mock_line_data.type = None
        mock_line_data.start_time = Mock()
        mock_line_data.start_time.dt = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        mock_line_data.end_time = Mock()
        mock_line_data.end_time.dt = datetime(2024, 1, 1, 6, 0, 0, tzinfo=timezone.utc)
        mock_line_data.wind_direction = Mock()
        mock_line_data.wind_direction.value = 270
        mock_line_data.wind_speed = Mock()
        mock_line_data.wind_speed.value = 10
        mock_line_data.wind_gust = None
        mock_line_data.visibility = Mock()
        mock_line_data.visibility.value = 6.0
        mock_line_data.clouds = []
        mock_line_data.flight_rules = 'VFR'
        
        mock_taf_obj = Mock()
        mock_taf_obj.data = Mock()
        mock_taf_obj.data.forecast = [mock_line_data]
        mock_taf_class.from_report.return_value = mock_taf_obj
        
        raw_taf = "TAF KJFK 010000Z 0100/0124 27010KT 6SM SCT020"
        taf_data = {'validTimeFrom': '2024-01-01T00:00:00Z', 'validTimeTo': '2024-01-02T00:00:00Z'}
        
        result = _parse_taf_from_raw(raw_taf, taf_data)
        assert len(result) == 1
        assert result[0]['windDirection'] == 270
        assert result[0]['windSpeed'] == 10
        assert result[0]['visibility'] == 6.0
    
    @patch('index.Taf')
    def test_parse_raw_taf_with_clouds(self, mock_taf_class):
        """Test parsing raw TAF with cloud layers."""
        # Mock cloud object
        mock_cloud = Mock()
        mock_cloud.repr = 'SCT'
        mock_cloud.base = 2000
        mock_cloud.type = None
        
        mock_line_data = Mock()
        mock_line_data.type = None
        mock_line_data.start_time = Mock()
        mock_line_data.start_time.dt = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        mock_line_data.end_time = Mock()
        mock_line_data.end_time.dt = datetime(2024, 1, 1, 6, 0, 0, tzinfo=timezone.utc)
        mock_line_data.wind_direction = None
        mock_line_data.wind_speed = None
        mock_line_data.wind_gust = None
        mock_line_data.visibility = Mock()
        mock_line_data.visibility.value = 6.0
        mock_line_data.clouds = [mock_cloud]
        mock_line_data.flight_rules = 'VFR'
        
        mock_taf_obj = Mock()
        mock_taf_obj.data = Mock()
        mock_taf_obj.data.forecast = [mock_line_data]
        mock_taf_class.from_report.return_value = mock_taf_obj
        
        raw_taf = "TAF KJFK 010000Z 0100/0124 6SM SCT020"
        taf_data = {}
        
        result = _parse_taf_from_raw(raw_taf, taf_data)
        assert len(result) == 1
        assert len(result[0]['skyConditions']) == 1
        assert result[0]['skyConditions'][0]['skyCover'] == 'SCT'
        assert result[0]['skyConditions'][0]['cloudBase'] == 2000
    
    @patch('index.Taf')
    def test_parse_raw_taf_with_visibility_string(self, mock_taf_class):
        """Test parsing raw TAF with visibility string format."""
        mock_line_data = Mock()
        mock_line_data.type = None
        mock_line_data.start_time = Mock()
        mock_line_data.start_time.dt = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        mock_line_data.end_time = Mock()
        mock_line_data.end_time.dt = datetime(2024, 1, 1, 6, 0, 0, tzinfo=timezone.utc)
        mock_line_data.wind_direction = None
        mock_line_data.wind_speed = None
        mock_line_data.wind_gust = None
        mock_line_data.visibility = Mock()
        mock_line_data.visibility.repr = 'P6SM'  # Greater than 6 SM
        mock_line_data.clouds = []
        mock_line_data.flight_rules = 'VFR'
        
        mock_taf_obj = Mock()
        mock_taf_obj.data = Mock()
        mock_taf_obj.data.forecast = [mock_line_data]
        mock_taf_class.from_report.return_value = mock_taf_obj
        
        raw_taf = "TAF KJFK 010000Z 0100/0124 P6SM"
        taf_data = {}
        
        result = _parse_taf_from_raw(raw_taf, taf_data)
        assert len(result) == 1
        assert result[0]['visibility'] == 6.1  # P6SM should parse to 6.1
    
    @patch('index.Taf')
    def test_parse_raw_taf_with_change_indicators(self, mock_taf_class):
        """Test parsing raw TAF with change indicators."""
        # FM (From) period
        mock_fm_data = Mock()
        mock_fm_data.type = 'FM'
        mock_fm_data.start_time = Mock()
        mock_fm_data.start_time.dt = datetime(2024, 1, 1, 6, 0, 0, tzinfo=timezone.utc)
        mock_fm_data.end_time = Mock()
        mock_fm_data.end_time.dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_fm_data.wind_direction = Mock()
        mock_fm_data.wind_direction.value = 180
        mock_fm_data.wind_speed = Mock()
        mock_fm_data.wind_speed.value = 15
        mock_fm_data.wind_gust = None
        mock_fm_data.visibility = Mock()
        mock_fm_data.visibility.value = 10.0
        mock_fm_data.clouds = []
        mock_fm_data.flight_rules = 'VFR'
        
        # TEMPO period
        mock_tempo_data = Mock()
        mock_tempo_data.type = 'TEMPO'
        mock_tempo_data.start_time = Mock()
        mock_tempo_data.start_time.dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_tempo_data.end_time = Mock()
        mock_tempo_data.end_time.dt = datetime(2024, 1, 1, 18, 0, 0, tzinfo=timezone.utc)
        mock_tempo_data.wind_direction = Mock()
        mock_tempo_data.wind_direction.value = 270
        mock_tempo_data.wind_speed = Mock()
        mock_tempo_data.wind_speed.value = 20
        mock_tempo_data.wind_gust = Mock()
        mock_tempo_data.wind_gust.value = 25
        mock_tempo_data.visibility = Mock()
        mock_tempo_data.visibility.value = 3.0
        mock_tempo_data.clouds = []
        mock_tempo_data.flight_rules = 'MVFR'
        
        mock_taf_obj = Mock()
        mock_taf_obj.data = Mock()
        mock_taf_obj.data.forecast = [mock_fm_data, mock_tempo_data]
        mock_taf_class.from_report.return_value = mock_taf_obj
        
        raw_taf = "TAF KJFK 010000Z 0100/0124 27010KT 6SM SCT020 FM010600 18015KT 10SM CLR TEMPO 1218 27020G25KT 3SM"
        taf_data = {}
        
        result = _parse_taf_from_raw(raw_taf, taf_data)
        assert len(result) == 2
        assert result[0]['changeIndicator'] == 'FM'
        assert result[1]['changeIndicator'] == 'TEMPO'
        assert result[1]['windGust'] == 25
    
    @patch('index.Taf')
    def test_parse_raw_taf_with_multiple_cloud_layers(self, mock_taf_class):
        """Test parsing raw TAF with multiple cloud layers."""
        mock_cloud1 = Mock()
        mock_cloud1.repr = 'FEW'
        mock_cloud1.base = 1000
        mock_cloud1.type = None
        
        mock_cloud2 = Mock()
        mock_cloud2.repr = 'SCT'
        mock_cloud2.base = 2000
        mock_cloud2.type = None
        
        mock_cloud3 = Mock()
        mock_cloud3.repr = 'BKN'
        mock_cloud3.base = 3000
        mock_cloud3.type = 'CB'
        
        mock_line_data = Mock()
        mock_line_data.type = None
        mock_line_data.start_time = Mock()
        mock_line_data.start_time.dt = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        mock_line_data.end_time = Mock()
        mock_line_data.end_time.dt = datetime(2024, 1, 1, 6, 0, 0, tzinfo=timezone.utc)
        mock_line_data.wind_direction = None
        mock_line_data.wind_speed = None
        mock_line_data.wind_gust = None
        mock_line_data.visibility = Mock()
        mock_line_data.visibility.value = 6.0
        mock_line_data.clouds = [mock_cloud1, mock_cloud2, mock_cloud3]
        mock_line_data.flight_rules = 'MVFR'
        
        mock_taf_obj = Mock()
        mock_taf_obj.data = Mock()
        mock_taf_obj.data.forecast = [mock_line_data]
        mock_taf_class.from_report.return_value = mock_taf_obj
        
        raw_taf = "TAF KJFK 010000Z 0100/0124 6SM FEW010 SCT020 BKN030CB"
        taf_data = {}
        
        result = _parse_taf_from_raw(raw_taf, taf_data)
        assert len(result) == 1
        assert len(result[0]['skyConditions']) == 3
        assert result[0]['skyConditions'][0]['skyCover'] == 'FEW'
        assert result[0]['skyConditions'][1]['skyCover'] == 'SCT'
        assert result[0]['skyConditions'][2]['skyCover'] == 'BKN'
        assert result[0]['skyConditions'][2]['cloudType'] == 'CB'
    
    @patch('index.Taf')
    def test_parse_raw_taf_avwx_error_fallback(self, mock_taf_class):
        """Test fallback when AVWX parsing fails."""
        mock_taf_class.from_report.side_effect = Exception("AVWX parsing error")
        
        raw_taf = "INVALID TAF"
        taf_data = {
            'validTimeFrom': '2024-01-01T00:00:00Z',
            'validTimeTo': '2024-01-02T00:00:00Z'
        }
        
        result = _parse_taf_from_raw(raw_taf, taf_data)
        # Should return fallback forecast
        assert len(result) == 1
        assert result[0]['fcstTimeFrom'] is not None
    
    @patch('index.Taf')
    def test_parse_raw_taf_no_forecast_periods(self, mock_taf_class):
        """Test fallback when AVWX parses but finds no forecast periods."""
        mock_taf_obj = Mock()
        mock_taf_obj.data = Mock()
        mock_taf_obj.data.forecast = []  # Empty forecast
        mock_taf_class.from_report.return_value = mock_taf_obj
        
        raw_taf = "TAF KJFK 010000Z 0100/0124"
        taf_data = {
            'validTimeFrom': '2024-01-01T00:00:00Z',
            'validTimeTo': '2024-01-02T00:00:00Z'
        }
        
        result = _parse_taf_from_raw(raw_taf, taf_data)
        # Should return fallback forecast
        assert len(result) == 1


class TestTAFForecastIntegration:
    """Integration tests for complete TAF forecast parsing."""
    
    def test_parse_with_structured_forecast_preferred(self):
        """Test that structured forecast is preferred over raw parsing."""
        taf = {
            'forecast': [{
                'fcstTimeFrom': '2024-01-01T00:00:00Z',
                'fcstTimeTo': '2024-01-01T06:00:00Z',
                'wdir': 270,
                'wspd': 10
            }],
            'rawTAF': 'TAF KJFK 010000Z 0100/0124 27010KT'
        }
        result = parse_taf_forecast(taf)
        # Should use structured forecast, not parse raw
        assert len(result) == 1
        assert result[0]['windDirection'] == 270
    
    def test_parse_falls_back_to_raw_when_no_structured(self):
        """Test that raw parsing is used when no structured forecast."""
        taf = {
            'rawTAF': 'TAF KJFK 010000Z 0100/0124 27010KT 6SM SCT020'
        }
        
        with patch('index._parse_taf_from_raw') as mock_parse_raw:
            mock_parse_raw.return_value = [{
                'fcstTimeFrom': '2024-01-01T00:00:00Z',
                'fcstTimeTo': '2024-01-01T06:00:00Z',
                'windDirection': 270,
                'windSpeed': 10
            }]
            result = parse_taf_forecast(taf)
            mock_parse_raw.assert_called_once()
            assert len(result) == 1
    
    def test_parse_falls_back_to_fallback_when_all_fail(self):
        """Test that fallback is used when structured and raw both fail."""
        taf = {
            'validTimeFrom': '2024-01-01T00:00:00Z',
            'validTimeTo': '2024-01-02T00:00:00Z'
        }
        
        with patch('index._parse_taf_from_raw') as mock_parse_raw:
            mock_parse_raw.return_value = []
            result = parse_taf_forecast(taf)
            assert len(result) == 1
            assert result[0]['fcstTimeFrom'] is not None


class TestEdgeCases:
    """Test edge cases and error handling."""
    
    def test_parse_empty_taf(self):
        """Test parsing empty TAF."""
        taf = {}
        result = parse_taf_forecast(taf)
        assert len(result) == 1  # Should return fallback
    
    def test_parse_taf_with_missing_wind(self):
        """Test parsing TAF with missing wind data."""
        taf = {
            'forecast': [{
                'fcstTimeFrom': '2024-01-01T00:00:00Z',
                'fcstTimeTo': '2024-01-01T06:00:00Z',
                'visib': 6.0
            }]
        }
        result = parse_taf_forecast(taf)
        assert len(result) == 1
        assert result[0]['windDirection'] is None
        assert result[0]['windSpeed'] is None
    
    def test_parse_taf_with_missing_visibility(self):
        """Test parsing TAF with missing visibility."""
        taf = {
            'forecast': [{
                'fcstTimeFrom': '2024-01-01T00:00:00Z',
                'fcstTimeTo': '2024-01-01T06:00:00Z',
                'wdir': 270,
                'wspd': 10
            }]
        }
        result = parse_taf_forecast(taf)
        assert len(result) == 1
        assert result[0]['visibility'] is None
    
    def test_parse_taf_with_missing_clouds(self):
        """Test parsing TAF with missing cloud data."""
        taf = {
            'forecast': [{
                'fcstTimeFrom': '2024-01-01T00:00:00Z',
                'fcstTimeTo': '2024-01-01T06:00:00Z',
                'wdir': 270,
                'wspd': 10,
                'visib': 6.0
            }]
        }
        result = parse_taf_forecast(taf)
        assert len(result) == 1
        assert result[0]['skyConditions'] == []
    
    def test_parse_taf_with_invalid_time_format(self):
        """Test parsing TAF with invalid time format."""
        taf = {
            'forecast': [{
                'fcstTimeFrom': 'invalid',
                'fcstTimeTo': 'invalid',
                'wdir': 270,
                'wspd': 10
            }]
        }
        result = parse_taf_forecast(taf)
        assert len(result) == 1
        # Time should still be set (even if invalid format)
        assert result[0]['fcstTimeFrom'] is not None


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

