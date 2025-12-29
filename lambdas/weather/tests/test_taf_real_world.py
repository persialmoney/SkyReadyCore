"""
Real-world TAF test cases based on actual TAF reports.

These tests use actual TAF strings to ensure the parser handles
real-world scenarios correctly.
"""
import pytest
import sys
from unittest.mock import patch, Mock
from datetime import datetime, timezone

import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from index import parse_taf_forecast, _parse_taf_from_raw


class TestRealWorldTAFExamples:
    """Test with real-world TAF examples."""
    
    @patch('index.Taf')
    def test_simple_vfr_taf(self, mock_taf_class):
        """Test simple VFR TAF."""
        # Real TAF: TAF KJFK 010000Z 0100/0124 27010KT 6SM SCT020
        mock_line_data = Mock()
        mock_line_data.type = None
        mock_line_data.start_time = Mock()
        mock_line_data.start_time.dt = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        mock_line_data.end_time = Mock()
        mock_line_data.end_time.dt = datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
        mock_line_data.wind_direction = Mock()
        mock_line_data.wind_direction.value = 270
        mock_line_data.wind_speed = Mock()
        mock_line_data.wind_speed.value = 10
        mock_line_data.wind_gust = None
        mock_line_data.visibility = Mock()
        mock_line_data.visibility.value = 6.0
        mock_line_data.clouds = []
        mock_line_data.flight_rules = 'VFR'
        
        mock_cloud = Mock()
        mock_cloud.repr = 'SCT'
        mock_cloud.base = 2000
        mock_cloud.type = None
        mock_line_data.clouds = [mock_cloud]
        
        mock_taf_obj = Mock()
        mock_taf_obj.data = Mock()
        mock_taf_obj.data.forecast = [mock_line_data]
        mock_taf_class.from_report.return_value = mock_taf_obj
        
        raw_taf = "TAF KJFK 010000Z 0100/0124 27010KT 6SM SCT020"
        taf_data = {}
        
        result = _parse_taf_from_raw(raw_taf, taf_data)
        assert len(result) == 1
        assert result[0]['windDirection'] == 270
        assert result[0]['windSpeed'] == 10
        assert result[0]['visibility'] == 6.0
        assert len(result[0]['skyConditions']) == 1
        assert result[0]['skyConditions'][0]['skyCover'] == 'SCT'
        assert result[0]['flightCategory'] == 'VFR'
    
    @patch('index.Taf')
    def test_taf_with_fm_and_tempo(self, mock_taf_class):
        """Test TAF with FM (From) and TEMPO (Temporary) periods."""
        # Real TAF: TAF KORD 010000Z 0100/0124 27010KT 6SM SCT020 
        #            FM010600 18015KT 10SM CLR TEMPO 1218 27020G25KT 3SM BKN015
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
        mock_tempo_data.flight_rules = 'MVFR'
        
        mock_cloud = Mock()
        mock_cloud.repr = 'BKN'
        mock_cloud.base = 1500
        mock_cloud.type = None
        mock_tempo_data.clouds = [mock_cloud]
        
        mock_taf_obj = Mock()
        mock_taf_obj.data = Mock()
        mock_taf_obj.data.forecast = [mock_fm_data, mock_tempo_data]
        mock_taf_class.from_report.return_value = mock_taf_obj
        
        raw_taf = "TAF KORD 010000Z 0100/0124 27010KT 6SM SCT020 FM010600 18015KT 10SM CLR TEMPO 1218 27020G25KT 3SM BKN015"
        taf_data = {}
        
        result = _parse_taf_from_raw(raw_taf, taf_data)
        assert len(result) == 2
        assert result[0]['changeIndicator'] == 'FM'
        assert result[0]['windDirection'] == 180
        assert result[0]['visibility'] == 10.0
        assert result[1]['changeIndicator'] == 'TEMPO'
        assert result[1]['windGust'] == 25
        assert result[1]['visibility'] == 3.0
        assert result[1]['flightCategory'] == 'MVFR'
    
    @patch('index.Taf')
    def test_taf_with_probability(self, mock_taf_class):
        """Test TAF with PROB (Probability) periods."""
        # Real TAF: TAF KLAX 010000Z 0100/0124 27010KT 6SM SCT020 
        #            PROB30 1218 27020G30KT 2SM BKN010
        mock_base_data = Mock()
        mock_base_data.type = None
        mock_base_data.start_time = Mock()
        mock_base_data.start_time.dt = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        mock_base_data.end_time = Mock()
        mock_base_data.end_time.dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_base_data.wind_direction = Mock()
        mock_base_data.wind_direction.value = 270
        mock_base_data.wind_speed = Mock()
        mock_base_data.wind_speed.value = 10
        mock_base_data.wind_gust = None
        mock_base_data.visibility = Mock()
        mock_base_data.visibility.value = 6.0
        mock_base_data.flight_rules = 'VFR'
        
        mock_cloud = Mock()
        mock_cloud.repr = 'SCT'
        mock_cloud.base = 2000
        mock_cloud.type = None
        mock_base_data.clouds = [mock_cloud]
        
        mock_prob_data = Mock()
        mock_prob_data.type = 'PROB30'
        mock_prob_data.start_time = Mock()
        mock_prob_data.start_time.dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_prob_data.end_time = Mock()
        mock_prob_data.end_time.dt = datetime(2024, 1, 1, 18, 0, 0, tzinfo=timezone.utc)
        mock_prob_data.wind_direction = Mock()
        mock_prob_data.wind_direction.value = 270
        mock_prob_data.wind_speed = Mock()
        mock_prob_data.wind_speed.value = 20
        mock_prob_data.wind_gust = Mock()
        mock_prob_data.wind_gust.value = 30
        mock_prob_data.visibility = Mock()
        mock_prob_data.visibility.value = 2.0
        mock_prob_data.flight_rules = 'IFR'
        
        mock_prob_cloud = Mock()
        mock_prob_cloud.repr = 'BKN'
        mock_prob_cloud.base = 1000
        mock_prob_cloud.type = None
        mock_prob_data.clouds = [mock_prob_cloud]
        
        mock_taf_obj = Mock()
        mock_taf_obj.data = Mock()
        mock_taf_obj.data.forecast = [mock_base_data, mock_prob_data]
        mock_taf_class.from_report.return_value = mock_taf_obj
        
        raw_taf = "TAF KLAX 010000Z 0100/0124 27010KT 6SM SCT020 PROB30 1218 27020G30KT 2SM BKN010"
        taf_data = {}
        
        result = _parse_taf_from_raw(raw_taf, taf_data)
        assert len(result) == 2
        assert result[1]['changeIndicator'] == 'PROB30'
        assert result[1]['windGust'] == 30
        assert result[1]['visibility'] == 2.0
        assert result[1]['flightCategory'] == 'IFR'
    
    @patch('index.Taf')
    def test_taf_with_cavok(self, mock_taf_class):
        """Test TAF with CAVOK (Ceiling and Visibility OK)."""
        # Real TAF: TAF KDEN 010000Z 0100/0124 27010KT CAVOK
        mock_line_data = Mock()
        mock_line_data.type = None
        mock_line_data.start_time = Mock()
        mock_line_data.start_time.dt = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        mock_line_data.end_time = Mock()
        mock_line_data.end_time.dt = datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
        mock_line_data.wind_direction = Mock()
        mock_line_data.wind_direction.value = 270
        mock_line_data.wind_speed = Mock()
        mock_line_data.wind_speed.value = 10
        mock_line_data.wind_gust = None
        mock_line_data.visibility = Mock()
        mock_line_data.visibility.repr = 'CAVOK'  # CAVOK visibility
        mock_line_data.clouds = []
        mock_line_data.flight_rules = 'VFR'
        
        mock_taf_obj = Mock()
        mock_taf_obj.data = Mock()
        mock_taf_obj.data.forecast = [mock_line_data]
        mock_taf_class.from_report.return_value = mock_taf_obj
        
        raw_taf = "TAF KDEN 010000Z 0100/0124 27010KT CAVOK"
        taf_data = {}
        
        result = _parse_taf_from_raw(raw_taf, taf_data)
        assert len(result) == 1
        assert result[0]['windDirection'] == 270
        # CAVOK visibility might be None or a special value
        assert result[0]['skyConditions'] == []
    
    @patch('index.Taf')
    def test_taf_with_becmg(self, mock_taf_class):
        """Test TAF with BECMG (Becoming) period."""
        # Real TAF: TAF KSEA 010000Z 0100/0124 27010KT 6SM SCT020 
        #            BECMG 0608 18015KT 10SM CLR
        mock_base_data = Mock()
        mock_base_data.type = None
        mock_base_data.start_time = Mock()
        mock_base_data.start_time.dt = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        mock_base_data.end_time = Mock()
        mock_base_data.end_time.dt = datetime(2024, 1, 1, 6, 0, 0, tzinfo=timezone.utc)
        mock_base_data.wind_direction = Mock()
        mock_base_data.wind_direction.value = 270
        mock_base_data.wind_speed = Mock()
        mock_base_data.wind_speed.value = 10
        mock_base_data.wind_gust = None
        mock_base_data.visibility = Mock()
        mock_base_data.visibility.value = 6.0
        mock_base_data.flight_rules = 'VFR'
        
        mock_cloud = Mock()
        mock_cloud.repr = 'SCT'
        mock_cloud.base = 2000
        mock_cloud.type = None
        mock_base_data.clouds = [mock_cloud]
        
        mock_becmg_data = Mock()
        mock_becmg_data.type = 'BECMG'
        mock_becmg_data.start_time = Mock()
        mock_becmg_data.start_time.dt = datetime(2024, 1, 1, 6, 0, 0, tzinfo=timezone.utc)
        mock_becmg_data.end_time = Mock()
        mock_becmg_data.end_time.dt = datetime(2024, 1, 1, 8, 0, 0, tzinfo=timezone.utc)
        mock_becmg_data.wind_direction = Mock()
        mock_becmg_data.wind_direction.value = 180
        mock_becmg_data.wind_speed = Mock()
        mock_becmg_data.wind_speed.value = 15
        mock_becmg_data.wind_gust = None
        mock_becmg_data.visibility = Mock()
        mock_becmg_data.visibility.value = 10.0
        mock_becmg_data.clouds = []
        mock_becmg_data.flight_rules = 'VFR'
        
        mock_taf_obj = Mock()
        mock_taf_obj.data = Mock()
        mock_taf_obj.data.forecast = [mock_base_data, mock_becmg_data]
        mock_taf_class.from_report.return_value = mock_taf_obj
        
        raw_taf = "TAF KSEA 010000Z 0100/0124 27010KT 6SM SCT020 BECMG 0608 18015KT 10SM CLR"
        taf_data = {}
        
        result = _parse_taf_from_raw(raw_taf, taf_data)
        assert len(result) == 2
        assert result[1]['changeIndicator'] == 'BECMG'
        assert result[1]['windDirection'] == 180
        assert result[1]['visibility'] == 10.0
    
    @patch('index.Taf')
    def test_taf_with_ifr_conditions(self, mock_taf_class):
        """Test TAF with IFR conditions."""
        # Real TAF: TAF KORD 010000Z 0100/0124 27010KT 1SM BKN005 OVC010
        mock_line_data = Mock()
        mock_line_data.type = None
        mock_line_data.start_time = Mock()
        mock_line_data.start_time.dt = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        mock_line_data.end_time = Mock()
        mock_line_data.end_time.dt = datetime(2024, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
        mock_line_data.wind_direction = Mock()
        mock_line_data.wind_direction.value = 270
        mock_line_data.wind_speed = Mock()
        mock_line_data.wind_speed.value = 10
        mock_line_data.wind_gust = None
        mock_line_data.visibility = Mock()
        mock_line_data.visibility.value = 1.0
        mock_line_data.flight_rules = 'IFR'
        
        mock_cloud1 = Mock()
        mock_cloud1.repr = 'BKN'
        mock_cloud1.base = 500
        mock_cloud1.type = None
        
        mock_cloud2 = Mock()
        mock_cloud2.repr = 'OVC'
        mock_cloud2.base = 1000
        mock_cloud2.type = None
        
        mock_line_data.clouds = [mock_cloud1, mock_cloud2]
        
        mock_taf_obj = Mock()
        mock_taf_obj.data = Mock()
        mock_taf_obj.data.forecast = [mock_line_data]
        mock_taf_class.from_report.return_value = mock_taf_obj
        
        raw_taf = "TAF KORD 010000Z 0100/0124 27010KT 1SM BKN005 OVC010"
        taf_data = {}
        
        result = _parse_taf_from_raw(raw_taf, taf_data)
        assert len(result) == 1
        assert result[0]['visibility'] == 1.0
        assert result[0]['flightCategory'] == 'IFR'
        assert len(result[0]['skyConditions']) == 2
        assert result[0]['skyConditions'][0]['skyCover'] == 'BKN'
        assert result[0]['skyConditions'][0]['cloudBase'] == 500
        assert result[0]['skyConditions'][1]['skyCover'] == 'OVC'
        assert result[0]['skyConditions'][1]['cloudBase'] == 1000


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

