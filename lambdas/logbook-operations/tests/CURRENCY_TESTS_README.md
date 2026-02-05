# Currency Calculation Tests

## Overview
Comprehensive test suite for FAA-compliant pilot currency calculations.

## Test File
`test_currency.py` - 400+ lines, 35+ test cases

## Quick Start

```bash
# Run all currency tests
./run_currency_tests.sh

# Or run directly
pytest test_currency.py -v

# With coverage
pytest test_currency.py --cov=../ --cov-report=html
```

## Test Coverage

### Day Currency (14 CFR 61.57(a))
- ✅ Current with 3+ landings in 90 days
- ✅ Expiring within 15 days
- ✅ Expired with insufficient landings
- ✅ Expired with no recent flights

### Night Currency (14 CFR 61.57(b))
- ✅ Current with 3+ night full-stop landings
- ✅ Expired with insufficient night landings
- ✅ Expired with insufficient full stops
- ✅ Requires BOTH night AND full-stop requirements

### IFR Currency (14 CFR 61.57(c))
- ✅ Current with 6 approaches + holds + tracking
- ✅ Expired with <6 approaches
- ✅ Expired missing holds
- ✅ Expired missing tracking
- ✅ Multiple missing requirements

### Flight Review (14 CFR 61.56)
- ✅ Current with review in last 24 months
- ✅ Expiring within 30 days
- ✅ Expired with no review found
- ✅ Expired with review over 24 months

### Medical Certificate (14 CFR 61.23)
- ✅ Class 1, 2, 3 expiration calculations
- ✅ Age-based expiration (over/under 40)
- ✅ Expiring within 30 days
- ✅ NOT_APPLICABLE handling

### Edge Cases
- ✅ Exactly 3 landings (minimum)
- ✅ Exactly 6 approaches (minimum)
- ✅ On expiration date (boundary)
- ✅ Error handling and connection cleanup

## Test Classes

```python
class TestDayCurrency         # 5 tests
class TestNightCurrency       # 4 tests
class TestIFRCurrency         # 6 tests
class TestFlightReview        # 4 tests
class TestMedicalCertificate  # 8 tests
class TestGetUserPilotInfo    # 3 tests
class TestHandleGetCurrency   # 3 tests
class TestEdgeCases           # 4 tests
```

## Running Specific Tests

```bash
# Run a single test class
pytest test_currency.py::TestDayCurrency -v

# Run a specific test
pytest test_currency.py::TestDayCurrency::test_current_with_recent_landings -v

# Run tests matching a pattern
pytest test_currency.py -k "expiring" -v
```

## Dependencies

```bash
pip install -r ../requirements-test.txt
```

Includes:
- pytest>=7.4.0
- pytest-cov>=4.1.0
- pytest-mock>=3.12.0
- boto3>=1.34.0
- psycopg2-binary>=2.9.9

## Coverage Report

After running with `--cov`, open the HTML report:
```bash
open htmlcov/index.html
```

Target: >90% coverage for currency calculation functions

## Mock Strategy

Tests use `unittest.mock` to mock:
- **Database cursors** - PostgreSQL queries
- **DynamoDB tables** - User pilot info
- **boto3 clients** - AWS SDK calls

Example:
```python
@pytest.fixture
def mock_cursor():
    cursor = MagicMock()
    cursor.fetchone = MagicMock()
    cursor.execute = MagicMock()
    return cursor
```

## Common Issues

### ImportError: No module named 'index'
```bash
# Solution: Run from tests directory
cd tests
pytest test_currency.py
```

### KeyError: 'total_landings'
```python
# Solution: Mock must return dict with expected keys
mock_cursor.fetchone.return_value = {
    'total_landings': 0,
    'last_landing_date': None
}
```

### AttributeError: 'MagicMock' has no attribute 'isoformat'
```python
# Solution: Use datetime.date objects, not strings
from datetime import date, timedelta
last_landing = date.today() - timedelta(days=30)
```

## CI/CD Integration

GitHub Actions workflow example:
```yaml
- name: Run currency tests
  run: |
    cd SkyReadyCore/lambdas/logbook-operations/tests
    pytest test_currency.py -v --cov --cov-report=xml
```

## See Also
- `/SkyReady/CURRENCY_TESTING.md` - Complete testing documentation
- `/run_currency_tests.sh` - Run all tests (backend + frontend)
- `/SkyReady/__tests__/` - Frontend currency tests
