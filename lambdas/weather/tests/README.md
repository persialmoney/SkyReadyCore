# TAF Parsing Tests

Comprehensive test suite for TAF (Terminal Aerodrome Forecast) parsing functionality.

## Test Files

### `test_taf_parsing.py`
Unit tests for individual parsing functions:
- Visibility parsing (`_parse_visibility_string`)
- Time conversion (`_convert_time_to_iso`)
- Sky conditions parsing (`_parse_taf_sky_conditions`)
- Fallback forecast creation (`_create_fallback_forecast`)
- Structured forecast parsing (`parse_taf_forecast`)
- AVWX raw TAF parsing (`_parse_taf_from_raw`)
- Edge cases and error handling

### `test_taf_real_world.py`
Integration tests using real-world TAF examples:
- Simple VFR TAF
- TAF with FM and TEMPO periods
- TAF with PROB (probability) periods
- TAF with CAVOK
- TAF with BECMG (becoming) periods
- TAF with IFR conditions

## Running Tests

```bash
# Install dependencies
cd /Users/naveen/dev/SkyReady/SkyReadyCore/lambdas/weather
pip install -r requirements.txt pytest

# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_taf_parsing.py -v

# Run with coverage
pytest tests/ --cov=index --cov-report=html
```

## Test Coverage

The tests cover:

1. **Visibility Parsing**
   - Regular SM format (6SM, 10SM)
   - Greater than format (P6SM)
   - Fractional format (1/2SM)
   - Empty/None values
   - Invalid formats

2. **Time Conversion**
   - Unix timestamps (seconds and milliseconds)
   - ISO strings
   - Empty/invalid formats

3. **Sky Conditions**
   - Single cloud layer
   - Multiple cloud layers
   - Clear sky (CLR)
   - Cloud types (CB, TCU)

4. **Forecast Parsing**
   - Structured forecast from AWC API
   - Raw TAF parsing with AVWX
   - Change indicators (FM, TEMPO, PROB, BECMG)
   - Fallback mechanisms

5. **Edge Cases**
   - Missing data fields
   - Invalid formats
   - Empty TAFs
   - Error handling

## Mocking

Tests use `unittest.mock` to mock the AVWX library, allowing tests to run without requiring actual TAF data or network access. This ensures:
- Fast test execution
- Deterministic results
- No external dependencies
- Easy to test edge cases

## Adding New Tests

When adding new TAF parsing features:

1. Add unit tests in `test_taf_parsing.py` for the specific function
2. Add integration tests in `test_taf_real_world.py` if it's a real-world scenario
3. Update this README if adding new test categories

## Example Test Structure

```python
class TestNewFeature:
    """Test description."""
    
    def test_basic_case(self):
        """Test basic functionality."""
        # Arrange
        input_data = {...}
        
        # Act
        result = function_under_test(input_data)
        
        # Assert
        assert result == expected_output
    
    def test_edge_case(self):
        """Test edge case."""
        # Test edge case
        pass
```

