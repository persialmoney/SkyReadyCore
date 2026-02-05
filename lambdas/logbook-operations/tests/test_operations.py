"""
Comprehensive tests for logbook-operations lambda.

Tests cover:
- Entry concatenation for embeddings
- CRUD operations (create, read, update, delete)
- List entries with filters
- Signature operations
- Database conversion
- Error handling
"""
import pytest
import sys
import os
from unittest.mock import Mock, patch, MagicMock, call
from datetime import datetime
import json

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from index import (
    concatenate_logbook_entry,
    handle_create_entry,
    handle_update_entry,
    handle_delete_entry,
    handle_get_entry,
    handle_list_entries,
    handle_request_signature,
    handle_sign_entry,
    convert_db_entry_to_graphql,
    handler
)


class TestConcatenateLogbookEntry:
    """Test logbook entry concatenation for embeddings."""
    
    def test_concatenate_basic_entry(self, sample_logbook_entry):
        """Test concatenating basic logbook entry."""
        result = concatenate_logbook_entry(sample_logbook_entry)
        assert 'Date: 2024-01-15' in result
        assert 'N12345' in result
        assert 'Total Time: 1.5 hours' in result
        assert 'Dual Received: 1.5 hours' in result
    
    def test_concatenate_with_aircraft_object(self, sample_logbook_entry):
        """Test concatenation with aircraft object."""
        result = concatenate_logbook_entry(sample_logbook_entry)
        assert 'Cessna 172' in result
        assert 'N12345' in result
        assert 'Single Engine Land' in result
    
    def test_concatenate_with_route_legs(self, sample_logbook_entry):
        """Test concatenation with route legs."""
        result = concatenate_logbook_entry(sample_logbook_entry)
        assert 'KJFK to KLGA' in result
        assert 'KLGA to KJFK' in result
    
    def test_concatenate_with_instructor(self, sample_logbook_entry):
        """Test concatenation with instructor info."""
        result = concatenate_logbook_entry(sample_logbook_entry)
        assert 'Instructor: John Smith' in result
        assert 'CFI12345' in result
    
    def test_concatenate_with_flight_types(self, sample_logbook_entry):
        """Test concatenation with flight types."""
        result = concatenate_logbook_entry(sample_logbook_entry)
        assert 'Flight Types: training, local' in result
    
    def test_concatenate_with_remarks(self, sample_logbook_entry):
        """Test concatenation with remarks."""
        result = concatenate_logbook_entry(sample_logbook_entry)
        assert 'Remarks: Good flight, practiced pattern work' in result
    
    def test_concatenate_minimal_entry(self):
        """Test concatenation with minimal data."""
        entry = {
            'date': '2024-01-15',
            'totalTime': 1.0
        }
        result = concatenate_logbook_entry(entry)
        assert 'Date: 2024-01-15' in result
        assert 'Total Time: 1.0 hours' in result
    
    def test_concatenate_with_landings_and_approaches(self):
        """Test concatenation with landings and approaches."""
        entry = {
            'date': '2024-01-15',
            'dayLandings': 5,
            'nightLandings': 2,
            'approaches': 3,
            'holds': True,
            'tracking': True
        }
        result = concatenate_logbook_entry(entry)
        assert 'Day Landings: 5' in result
        assert 'Night Landings: 2' in result
        assert 'Approaches: 3' in result
        assert 'Holds: Performed' in result
        assert 'Tracking/Intercepting: Performed' in result
    
    def test_concatenate_with_safety_notes(self):
        """Test concatenation with safety notes."""
        entry = {
            'date': '2024-01-15',
            'safetyNotes': 'Encountered wake turbulence',
            'safetyRelevant': True
        }
        result = concatenate_logbook_entry(entry)
        assert 'Safety Notes: Encountered wake turbulence' in result
        assert 'Safety Relevant: Yes' in result


class TestConvertDbEntryToGraphQL:
    """Test database entry to GraphQL conversion."""
    
    def test_convert_complete_entry(self, sample_db_entry):
        """Test converting complete entry."""
        result = convert_db_entry_to_graphql(sample_db_entry)
        assert result['entryId'] == 'entry-123'
        assert result['userId'] == 'user-456'
        assert result['tailNumber'] == 'N12345'
        assert result['totalTime'] == 1.5
        assert result['dualReceived'] == 1.5
        assert result['status'] == 'draft'
    
    def test_convert_with_datetime_objects(self, sample_db_entry):
        """Test conversion handles datetime objects."""
        result = convert_db_entry_to_graphql(sample_db_entry)
        assert '2024-01-15' in result['date']
        assert '2024-01-15' in result['createdAt']
        assert '2024-01-15' in result['updatedAt']
    
    def test_convert_with_none_values(self):
        """Test conversion handles None values."""
        entry = {
            'entry_id': 'entry-123',
            'user_id': 'user-456',
            'date': datetime(2024, 1, 15),
            'aircraft': None,
            'tail_number': None,
            'route_legs': [],
            'route': None,
            'flight_types': [],
            'total_time': None,
            'pic': None,
            'created_at': datetime(2024, 1, 15),
            'updated_at': datetime(2024, 1, 15)
        }
        result = convert_db_entry_to_graphql(entry)
        assert result['aircraft'] is None
        assert result['totalTime'] is None
        assert result['pic'] is None
    
    def test_convert_with_nested_objects(self, sample_db_entry):
        """Test conversion handles nested objects."""
        result = convert_db_entry_to_graphql(sample_db_entry)
        assert result['aircraft']['id'] == 'aircraft-123'
        assert result['instructor']['name'] == 'John Smith'
        assert len(result['routeLegs']) == 2


class TestHandleCreateEntry:
    """Test create logbook entry handler."""
    
    @patch('index.generate_embedding')
    @patch('index.get_db_connection')
    @patch('index.return_db_connection')
    def test_create_entry_success(
        self, 
        mock_return_conn, 
        mock_get_conn, 
        mock_embedding,
        mock_db_connection,
        sample_logbook_entry,
        sample_db_entry
    ):
        """Test successful entry creation."""
        conn, cursor = mock_db_connection
        mock_get_conn.return_value = conn
        mock_embedding.return_value = [0.1, 0.2, 0.3]
        cursor.fetchone.return_value = sample_db_entry
        
        arguments = {'input': sample_logbook_entry}
        result = handle_create_entry('user-456', arguments)
        
        # Verify embedding was generated
        mock_embedding.assert_called_once()
        
        # Verify database insertion
        assert cursor.execute.call_count == 3  # INSERT entry, INSERT embedding, SELECT entry
        conn.commit.assert_called_once()
        
        # Verify result
        assert result['userId'] == 'user-456'
        assert result['totalTime'] == 1.5
    
    @patch('index.generate_embedding')
    @patch('index.get_db_connection')
    @patch('index.return_db_connection')
    def test_create_entry_missing_input(
        self,
        mock_return_conn,
        mock_get_conn,
        mock_embedding
    ):
        """Test create entry with missing input."""
        with pytest.raises(ValueError, match="input is required"):
            handle_create_entry('user-456', {})
    
    @patch('index.generate_embedding')
    @patch('index.get_db_connection')
    @patch('index.return_db_connection')
    def test_create_entry_db_error_rollback(
        self,
        mock_return_conn,
        mock_get_conn,
        mock_embedding,
        mock_db_connection,
        sample_logbook_entry
    ):
        """Test entry creation with database error triggers rollback."""
        conn, cursor = mock_db_connection
        mock_get_conn.return_value = conn
        mock_embedding.return_value = [0.1, 0.2, 0.3]
        cursor.execute.side_effect = Exception("Database error")
        
        arguments = {'input': sample_logbook_entry}
        
        with pytest.raises(Exception):
            handle_create_entry('user-456', arguments)
        
        conn.rollback.assert_called_once()


class TestHandleUpdateEntry:
    """Test update logbook entry handler."""
    
    @patch('index.generate_embedding')
    @patch('index.get_db_connection')
    @patch('index.return_db_connection')
    def test_update_entry_success(
        self,
        mock_return_conn,
        mock_get_conn,
        mock_embedding,
        mock_db_connection,
        sample_db_entry
    ):
        """Test successful entry update."""
        conn, cursor = mock_db_connection
        mock_get_conn.return_value = conn
        mock_embedding.return_value = [0.1, 0.2, 0.3]
        
        # Mock existing entry fetch
        cursor.fetchone.side_effect = [sample_db_entry, sample_db_entry]
        
        arguments = {
            'entryId': 'entry-123',
            'input': {'remarks': 'Updated remarks', 'totalTime': 2.0}
        }
        
        result = handle_update_entry('user-456', arguments)
        
        # Verify embedding was regenerated
        mock_embedding.assert_called_once()
        
        # Verify database update
        conn.commit.assert_called_once()
        
        # Verify result
        assert result is not None
    
    @patch('index.get_db_connection')
    def test_update_entry_missing_entry_id(self, mock_get_conn):
        """Test update with missing entry ID."""
        with pytest.raises(ValueError, match="entryId is required"):
            handle_update_entry('user-456', {'input': {}})
    
    @patch('index.get_db_connection')
    def test_update_entry_missing_input(self, mock_get_conn):
        """Test update with missing input."""
        with pytest.raises(ValueError, match="input is required"):
            handle_update_entry('user-456', {'entryId': 'entry-123'})
    
    @patch('index.get_db_connection')
    @patch('index.return_db_connection')
    def test_update_entry_not_found(
        self,
        mock_return_conn,
        mock_get_conn,
        mock_db_connection
    ):
        """Test update with non-existent entry."""
        conn, cursor = mock_db_connection
        mock_get_conn.return_value = conn
        cursor.fetchone.return_value = None
        
        arguments = {
            'entryId': 'nonexistent',
            'input': {'remarks': 'Updated'}
        }
        
        with pytest.raises(ValueError, match="not found"):
            handle_update_entry('user-456', arguments)


class TestHandleDeleteEntry:
    """Test delete logbook entry handler."""
    
    @patch('index.get_db_connection')
    @patch('index.return_db_connection')
    def test_delete_entry_success(
        self,
        mock_return_conn,
        mock_get_conn,
        mock_db_connection
    ):
        """Test successful entry deletion."""
        conn, cursor = mock_db_connection
        mock_get_conn.return_value = conn
        cursor.rowcount = 1
        
        arguments = {'entryId': 'entry-123'}
        result = handle_delete_entry('user-456', arguments)
        
        # Verify both deletes were called (embedding and entry)
        assert cursor.execute.call_count == 2
        conn.commit.assert_called_once()
        assert result is True
    
    @patch('index.get_db_connection')
    def test_delete_entry_missing_id(self, mock_get_conn):
        """Test delete with missing entry ID."""
        with pytest.raises(ValueError, match="entryId is required"):
            handle_delete_entry('user-456', {})
    
    @patch('index.get_db_connection')
    @patch('index.return_db_connection')
    def test_delete_entry_not_found(
        self,
        mock_return_conn,
        mock_get_conn,
        mock_db_connection
    ):
        """Test delete with non-existent entry."""
        conn, cursor = mock_db_connection
        mock_get_conn.return_value = conn
        cursor.rowcount = 0
        
        arguments = {'entryId': 'nonexistent'}
        
        with pytest.raises(ValueError, match="not found"):
            handle_delete_entry('user-456', arguments)


class TestHandleGetEntry:
    """Test get logbook entry handler."""
    
    @patch('index.get_db_connection')
    @patch('index.return_db_connection')
    def test_get_entry_success(
        self,
        mock_return_conn,
        mock_get_conn,
        mock_db_connection,
        sample_db_entry
    ):
        """Test successful entry retrieval."""
        conn, cursor = mock_db_connection
        mock_get_conn.return_value = conn
        cursor.fetchone.return_value = sample_db_entry
        
        arguments = {'entryId': 'entry-123'}
        result = handle_get_entry('user-456', arguments)
        
        assert result is not None
        assert result['entryId'] == 'entry-123'
        assert result['userId'] == 'user-456'
    
    @patch('index.get_db_connection')
    def test_get_entry_missing_id(self, mock_get_conn):
        """Test get with missing entry ID."""
        with pytest.raises(ValueError, match="entryId is required"):
            handle_get_entry('user-456', {})
    
    @patch('index.get_db_connection')
    @patch('index.return_db_connection')
    def test_get_entry_not_found(
        self,
        mock_return_conn,
        mock_get_conn,
        mock_db_connection
    ):
        """Test get with non-existent entry."""
        conn, cursor = mock_db_connection
        mock_get_conn.return_value = conn
        cursor.fetchone.return_value = None
        
        arguments = {'entryId': 'nonexistent'}
        result = handle_get_entry('user-456', arguments)
        
        assert result is None


class TestHandleListEntries:
    """Test list logbook entries handler."""
    
    @patch('index.get_db_connection')
    @patch('index.return_db_connection')
    def test_list_entries_success(
        self,
        mock_return_conn,
        mock_get_conn,
        mock_db_connection,
        sample_db_entry
    ):
        """Test successful entries listing."""
        conn, cursor = mock_db_connection
        mock_get_conn.return_value = conn
        cursor.fetchall.return_value = [sample_db_entry]
        
        arguments = {}
        result = handle_list_entries('user-456', arguments)
        
        assert 'items' in result
        assert len(result['items']) == 1
        assert result['items'][0]['entryId'] == 'entry-123'
    
    @patch('index.get_db_connection')
    @patch('index.return_db_connection')
    def test_list_entries_with_date_filter(
        self,
        mock_return_conn,
        mock_get_conn,
        mock_db_connection,
        sample_db_entry
    ):
        """Test list with date range filter."""
        conn, cursor = mock_db_connection
        mock_get_conn.return_value = conn
        cursor.fetchall.return_value = [sample_db_entry]
        
        arguments = {
            'filters': {
                'dateFrom': '2024-01-01',
                'dateTo': '2024-01-31'
            }
        }
        
        result = handle_list_entries('user-456', arguments)
        
        assert len(result['items']) == 1
        # Verify query includes date filters
        cursor.execute.assert_called_once()
    
    @patch('index.get_db_connection')
    @patch('index.return_db_connection')
    def test_list_entries_with_status_filter(
        self,
        mock_return_conn,
        mock_get_conn,
        mock_db_connection,
        sample_db_entry
    ):
        """Test list with status filter."""
        conn, cursor = mock_db_connection
        mock_get_conn.return_value = conn
        cursor.fetchall.return_value = [sample_db_entry]
        
        arguments = {
            'filters': {'status': 'signed'}
        }
        
        result = handle_list_entries('user-456', arguments)
        assert len(result['items']) == 1
    
    @patch('index.get_db_connection')
    @patch('index.return_db_connection')
    def test_list_entries_pagination(
        self,
        mock_return_conn,
        mock_get_conn,
        mock_db_connection,
        sample_db_entry
    ):
        """Test list with pagination."""
        conn, cursor = mock_db_connection
        mock_get_conn.return_value = conn
        
        # Return 6 entries (limit is 5)
        cursor.fetchall.return_value = [sample_db_entry] * 6
        
        arguments = {'limit': 5}
        result = handle_list_entries('user-456', arguments)
        
        # Should return 5 items and a nextToken
        assert len(result['items']) == 5
        assert result['nextToken'] is not None
    
    @patch('index.get_db_connection')
    @patch('index.return_db_connection')
    def test_list_entries_with_null_limit(
        self,
        mock_return_conn,
        mock_get_conn,
        mock_db_connection,
        sample_db_entry
    ):
        """Test list with None/null limit (from GraphQL)."""
        conn, cursor = mock_db_connection
        mock_get_conn.return_value = conn
        cursor.fetchall.return_value = [sample_db_entry]
        
        # Simulate GraphQL passing limit: null (becomes None in Python)
        arguments = {'limit': None}
        result = handle_list_entries('user-456', arguments)
        
        # Should use default limit of 50 and not crash
        assert 'items' in result
        assert len(result['items']) == 1
        cursor.execute.assert_called_once()
        
        # Verify the query was called with default limit + 1 (51)
        call_args = cursor.execute.call_args
        assert call_args[0][1][-1] == 51  # Last param should be limit + 1


class TestHandleSignatureOperations:
    """Test signature-related operations."""
    
    @patch('index.get_db_connection')
    @patch('index.return_db_connection')
    def test_request_signature_success(
        self,
        mock_return_conn,
        mock_get_conn,
        mock_db_connection,
        sample_db_entry
    ):
        """Test successful signature request."""
        conn, cursor = mock_db_connection
        mock_get_conn.return_value = conn
        sample_db_entry_copy = sample_db_entry.copy()
        sample_db_entry_copy['status'] = 'pending-signature'
        cursor.fetchone.return_value = sample_db_entry_copy
        
        arguments = {'entryId': 'entry-123', 'instructorId': 'instructor-123'}
        result = handle_request_signature('user-456', arguments)
        
        conn.commit.assert_called_once()
        assert result['status'] == 'pending-signature'
    
    @patch('index.get_db_connection')
    def test_request_signature_missing_entry_id(self, mock_get_conn):
        """Test request signature with missing entry ID."""
        with pytest.raises(ValueError, match="entryId is required"):
            handle_request_signature('user-456', {})
    
    @patch('index.get_db_connection')
    @patch('index.return_db_connection')
    def test_sign_entry_success(
        self,
        mock_return_conn,
        mock_get_conn,
        mock_db_connection,
        sample_db_entry
    ):
        """Test successful entry signing."""
        conn, cursor = mock_db_connection
        mock_get_conn.return_value = conn
        sample_db_entry_copy = sample_db_entry.copy()
        sample_db_entry_copy['status'] = 'signed'
        sample_db_entry_copy['signature'] = {'signedBy': 'instructor-123'}
        cursor.fetchone.return_value = sample_db_entry_copy
        
        arguments = {
            'entryId': 'entry-123',
            'signature': {
                'signedBy': 'instructor-123',
                'signatureImage': 'base64-image-data'
            }
        }
        
        result = handle_sign_entry('user-456', arguments)
        
        conn.commit.assert_called_once()
        assert result['status'] == 'signed'
    
    @patch('index.get_db_connection')
    def test_sign_entry_missing_signature(self, mock_get_conn):
        """Test sign entry with missing signature."""
        with pytest.raises(ValueError, match="signature is required"):
            handle_sign_entry('user-456', {'entryId': 'entry-123'})


class TestHandler:
    """Test main Lambda handler."""
    
    @patch('index.handle_create_entry')
    def test_handler_create_entry(self, mock_create, sample_appsync_event):
        """Test handler routes to create entry."""
        mock_create.return_value = {'entryId': 'entry-123'}
        
        result = handler(sample_appsync_event, None)
        
        mock_create.assert_called_once_with('user-456', sample_appsync_event['arguments'])
        assert result['entryId'] == 'entry-123'
    
    @patch('index.handle_get_entry')
    def test_handler_get_entry(self, mock_get):
        """Test handler routes to get entry."""
        event = {
            'identity': {'sub': 'user-456'},
            'arguments': {'entryId': 'entry-123'},
            'info': {'fieldName': 'getLogbookEntry'}
        }
        mock_get.return_value = {'entryId': 'entry-123'}
        
        result = handler(event, None)
        
        mock_get.assert_called_once_with('user-456', event['arguments'])
    
    @patch('index.handle_update_entry')
    def test_handler_update_entry(self, mock_update):
        """Test handler routes to update entry."""
        event = {
            'identity': {'sub': 'user-456'},
            'arguments': {'entryId': 'entry-123', 'input': {}},
            'info': {'fieldName': 'updateLogbookEntry'}
        }
        mock_update.return_value = {'entryId': 'entry-123'}
        
        result = handler(event, None)
        
        mock_update.assert_called_once()
    
    @patch('index.handle_delete_entry')
    def test_handler_delete_entry(self, mock_delete):
        """Test handler routes to delete entry."""
        event = {
            'identity': {'sub': 'user-456'},
            'arguments': {'entryId': 'entry-123'},
            'info': {'fieldName': 'deleteLogbookEntry'}
        }
        mock_delete.return_value = True
        
        result = handler(event, None)
        
        mock_delete.assert_called_once()
        assert result is True
    
    @patch('index.handle_list_entries')
    def test_handler_list_entries(self, mock_list):
        """Test handler routes to list entries."""
        event = {
            'identity': {'sub': 'user-456'},
            'arguments': {},
            'info': {'fieldName': 'listLogbookEntries'}
        }
        mock_list.return_value = {'items': []}
        
        result = handler(event, None)
        
        mock_list.assert_called_once()
    
    def test_handler_missing_user_id(self):
        """Test handler with missing user ID."""
        event = {
            'identity': {},
            'arguments': {},
            'info': {'fieldName': 'createLogbookEntry'}
        }
        
        with pytest.raises(Exception, match="User ID"):
            handler(event, None)
    
    def test_handler_unknown_field_name(self):
        """Test handler with unknown field name."""
        event = {
            'identity': {'sub': 'user-456'},
            'arguments': {},
            'info': {'fieldName': 'unknownOperation'}
        }
        
        with pytest.raises(Exception, match="Unknown field name"):
            handler(event, None)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
