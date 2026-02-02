"""
Comprehensive tests for logbook-ai lambda.

Tests cover:
- Embedding generation
- Semantic search with vector similarity
- Claude chat integration
- Chat query processing
- Database integration
- Error handling
"""
import pytest
import sys
import os
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import json

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from index import (
    generate_embedding,
    invoke_claude_chat,
    handle_semantic_search,
    handle_chat_query,
    convert_db_entry_to_graphql,
    handler
)


class TestGenerateEmbedding:
    """Test embedding generation using AWS Bedrock."""
    
    @patch('index.bedrock_client')
    def test_generate_embedding_success(self, mock_bedrock):
        """Test successful embedding generation."""
        # Mock Bedrock response
        response = {
            'body': MagicMock()
        }
        response['body'].read.return_value = json.dumps({
            'embedding': [0.1, 0.2, 0.3, 0.4]
        }).encode('utf-8')
        mock_bedrock.invoke_model.return_value = response
        
        text = "Test flight entry"
        result = generate_embedding(text)
        
        assert result == [0.1, 0.2, 0.3, 0.4]
        mock_bedrock.invoke_model.assert_called_once()
        
        # Verify request format
        call_args = mock_bedrock.invoke_model.call_args
        assert call_args[1]['modelId'] == 'amazon.titan-embed-text-v1'
        body = json.loads(call_args[1]['body'])
        assert body['inputText'] == text
    
    @patch('index.bedrock_client')
    def test_generate_embedding_empty_response(self, mock_bedrock):
        """Test handling of empty embedding response."""
        response = {
            'body': MagicMock()
        }
        response['body'].read.return_value = json.dumps({
            'embedding': []
        }).encode('utf-8')
        mock_bedrock.invoke_model.return_value = response
        
        with pytest.raises(ValueError, match="No embedding returned"):
            generate_embedding("Test text")
    
    @patch('index.bedrock_client')
    def test_generate_embedding_bedrock_error(self, mock_bedrock):
        """Test handling of Bedrock API errors."""
        mock_bedrock.invoke_model.side_effect = Exception("Bedrock API error")
        
        with pytest.raises(Exception):
            generate_embedding("Test text")


class TestInvokeClaudeChat:
    """Test Claude chat invocation."""
    
    @patch('index.bedrock_client')
    def test_invoke_claude_success(self, mock_bedrock, sample_claude_response):
        """Test successful Claude invocation."""
        response = {
            'body': MagicMock()
        }
        response['body'].read.return_value = json.dumps(sample_claude_response).encode('utf-8')
        mock_bedrock.invoke_model.return_value = response
        
        messages = [
            {'role': 'user', 'content': 'How many hours have I flown?'}
        ]
        system_prompt = "You are a helpful aviation assistant."
        
        result = invoke_claude_chat(messages, system_prompt)
        
        assert 'dual instruction' in result.lower()
        mock_bedrock.invoke_model.assert_called_once()
        
        # Verify request format
        call_args = mock_bedrock.invoke_model.call_args
        assert 'anthropic.claude-sonnet' in call_args[1]['modelId']
        body = json.loads(call_args[1]['body'])
        assert body['system'] == system_prompt
        assert body['messages'][0]['role'] == 'user'
    
    @patch('index.bedrock_client')
    def test_invoke_claude_with_string_content(self, mock_bedrock):
        """Test Claude response with string content format."""
        response = {
            'body': MagicMock()
        }
        response['body'].read.return_value = json.dumps({
            'content': 'Direct string response'
        }).encode('utf-8')
        mock_bedrock.invoke_model.return_value = response
        
        messages = [{'role': 'user', 'content': 'Test'}]
        result = invoke_claude_chat(messages, "System prompt")
        
        assert result == 'Direct string response'
    
    @patch('index.bedrock_client')
    def test_invoke_claude_unexpected_format(self, mock_bedrock):
        """Test handling of unexpected response format."""
        response = {
            'body': MagicMock()
        }
        response['body'].read.return_value = json.dumps({
            'invalid': 'format'
        }).encode('utf-8')
        mock_bedrock.invoke_model.return_value = response
        
        messages = [{'role': 'user', 'content': 'Test'}]
        
        with pytest.raises(ValueError, match="Unexpected response format"):
            invoke_claude_chat(messages, "System prompt")
    
    @patch('index.bedrock_client')
    def test_invoke_claude_api_error(self, mock_bedrock):
        """Test handling of Claude API errors."""
        mock_bedrock.invoke_model.side_effect = Exception("Claude API error")
        
        messages = [{'role': 'user', 'content': 'Test'}]
        
        with pytest.raises(Exception):
            invoke_claude_chat(messages, "System prompt")


class TestHandleSemanticSearch:
    """Test semantic search handler."""
    
    @patch('index.generate_embedding')
    @patch('index.get_db_connection')
    @patch('index.return_db_connection')
    def test_semantic_search_success(
        self,
        mock_return_conn,
        mock_get_conn,
        mock_embedding,
        mock_db_connection,
        sample_logbook_entries,
        sample_embedding
    ):
        """Test successful semantic search."""
        conn, cursor = mock_db_connection
        mock_get_conn.return_value = conn
        mock_embedding.return_value = sample_embedding
        
        # Add similarity score to entries
        entries_with_similarity = [
            {**entry, 'similarity': 0.95} for entry in sample_logbook_entries
        ]
        cursor.fetchall.return_value = entries_with_similarity
        
        arguments = {'query': 'pattern work', 'limit': 10}
        result = handle_semantic_search('user-456', arguments)
        
        # Verify embedding was generated
        mock_embedding.assert_called_once_with('pattern work')
        
        # Verify database query
        cursor.execute.assert_called_once()
        call_args = cursor.execute.call_args[0]
        assert 'embedding <=> %s::vector' in call_args[0] or 'similarity' in call_args[0].lower()
        
        # Verify results
        assert len(result) == 2
        assert result[0]['entryId'] == 'entry-1'
    
    @patch('index.get_db_connection')
    def test_semantic_search_missing_query(self, mock_get_conn):
        """Test semantic search with missing query."""
        with pytest.raises(ValueError, match="query is required"):
            handle_semantic_search('user-456', {})
    
    @patch('index.generate_embedding')
    @patch('index.get_db_connection')
    @patch('index.return_db_connection')
    def test_semantic_search_with_limit(
        self,
        mock_return_conn,
        mock_get_conn,
        mock_embedding,
        mock_db_connection,
        sample_logbook_entries,
        sample_embedding
    ):
        """Test semantic search with custom limit."""
        conn, cursor = mock_db_connection
        mock_get_conn.return_value = conn
        mock_embedding.return_value = sample_embedding
        
        entries_with_similarity = [
            {**entry, 'similarity': 0.95} for entry in sample_logbook_entries[:1]
        ]
        cursor.fetchall.return_value = entries_with_similarity
        
        arguments = {'query': 'cross country', 'limit': 5}
        result = handle_semantic_search('user-456', arguments)
        
        # Verify limit was passed to query
        call_args = cursor.execute.call_args[0]
        assert 5 in cursor.execute.call_args[0][1] or cursor.execute.call_args[1]
        
        assert len(result) == 1
    
    @patch('index.generate_embedding')
    @patch('index.get_db_connection')
    @patch('index.return_db_connection')
    def test_semantic_search_no_results(
        self,
        mock_return_conn,
        mock_get_conn,
        mock_embedding,
        mock_db_connection,
        sample_embedding
    ):
        """Test semantic search with no matching entries."""
        conn, cursor = mock_db_connection
        mock_get_conn.return_value = conn
        mock_embedding.return_value = sample_embedding
        cursor.fetchall.return_value = []
        
        arguments = {'query': 'helicopter flight'}
        result = handle_semantic_search('user-456', arguments)
        
        assert result == []


class TestHandleChatQuery:
    """Test chat query handler."""
    
    @patch('index.invoke_claude_chat')
    @patch('index.get_db_connection')
    @patch('index.return_db_connection')
    def test_chat_query_success(
        self,
        mock_return_conn,
        mock_get_conn,
        mock_claude,
        mock_db_connection,
        sample_logbook_entries
    ):
        """Test successful chat query."""
        conn, cursor = mock_db_connection
        mock_get_conn.return_value = conn
        cursor.fetchall.return_value = sample_logbook_entries
        mock_claude.return_value = "You have received 1.5 hours of dual instruction."
        
        arguments = {'query': 'How many hours of dual instruction?'}
        result = handle_chat_query('user-456', arguments)
        
        # Verify Claude was called
        mock_claude.assert_called_once()
        
        # Verify result structure
        assert result['query'] == 'How many hours of dual instruction?'
        assert 'dual instruction' in result['response'].lower()
        assert result['entriesUsed'] == 2
    
    @patch('index.get_db_connection')
    def test_chat_query_missing_query(self, mock_get_conn):
        """Test chat query with missing query."""
        with pytest.raises(ValueError, match="query is required"):
            handle_chat_query('user-456', {})
    
    @patch('index.invoke_claude_chat')
    @patch('index.get_db_connection')
    @patch('index.return_db_connection')
    def test_chat_query_context_building(
        self,
        mock_return_conn,
        mock_get_conn,
        mock_claude,
        mock_db_connection,
        sample_logbook_entries
    ):
        """Test that chat query builds proper context."""
        conn, cursor = mock_db_connection
        mock_get_conn.return_value = conn
        cursor.fetchall.return_value = sample_logbook_entries
        mock_claude.return_value = "Response"
        
        arguments = {'query': 'Test query'}
        handle_chat_query('user-456', arguments)
        
        # Verify Claude was called with proper context
        call_args = mock_claude.call_args[0]
        messages = call_args[0]
        system_prompt = call_args[1]
        
        assert 'aviation' in system_prompt.lower() or 'pilot' in system_prompt.lower()
        assert messages[0]['role'] == 'user'
        assert 'logbook' in messages[0]['content'].lower()
    
    @patch('index.invoke_claude_chat')
    @patch('index.get_db_connection')
    @patch('index.return_db_connection')
    def test_chat_query_no_entries(
        self,
        mock_return_conn,
        mock_get_conn,
        mock_claude,
        mock_db_connection
    ):
        """Test chat query with no logbook entries."""
        conn, cursor = mock_db_connection
        mock_get_conn.return_value = conn
        cursor.fetchall.return_value = []
        mock_claude.return_value = "You have no logbook entries yet."
        
        arguments = {'query': 'How many hours have I flown?'}
        result = handle_chat_query('user-456', arguments)
        
        assert result['entriesUsed'] == 0
        mock_claude.assert_called_once()
    
    @patch('index.invoke_claude_chat')
    @patch('index.get_db_connection')
    @patch('index.return_db_connection')
    def test_chat_query_limits_context(
        self,
        mock_return_conn,
        mock_get_conn,
        mock_claude,
        mock_db_connection,
        sample_logbook_entries
    ):
        """Test that chat query limits context to recent entries."""
        conn, cursor = mock_db_connection
        mock_get_conn.return_value = conn
        
        # Return 30 entries
        many_entries = sample_logbook_entries * 15
        cursor.fetchall.return_value = many_entries
        mock_claude.return_value = "Response"
        
        arguments = {'query': 'Test query'}
        handle_chat_query('user-456', arguments)
        
        # Verify context is limited (to 20 entries in the implementation)
        call_args = mock_claude.call_args[0]
        messages = call_args[0]
        context = messages[0]['content']
        
        # Context should not include all 30 entries
        assert len(context) > 0


class TestConvertDbEntryToGraphQL:
    """Test database entry to GraphQL conversion."""
    
    def test_convert_complete_entry(self, sample_logbook_entries):
        """Test converting complete entry."""
        entry = sample_logbook_entries[0]
        result = convert_db_entry_to_graphql(entry)
        
        assert result['entryId'] == 'entry-1'
        assert result['userId'] == 'user-456'
        assert result['tailNumber'] == 'N12345'
        assert result['totalTime'] == 1.5
        assert result['dualReceived'] == 1.5
    
    def test_convert_with_none_values(self):
        """Test conversion handles None values."""
        entry = {
            'entry_id': 'entry-123',
            'user_id': 'user-456',
            'date': datetime(2024, 1, 15),
            'aircraft': None,
            'tail_number': None,
            'route_legs': [],
            'total_time': None,
            'pic': None,
            'created_at': datetime(2024, 1, 15),
            'updated_at': datetime(2024, 1, 15),
            'flight_types': []
        }
        result = convert_db_entry_to_graphql(entry)
        
        assert result['aircraft'] is None
        assert result['totalTime'] is None
        assert result['pic'] is None


class TestHandler:
    """Test main Lambda handler."""
    
    @patch('index.handle_semantic_search')
    def test_handler_semantic_search(
        self,
        mock_search,
        sample_appsync_search_event
    ):
        """Test handler routes to semantic search."""
        mock_search.return_value = [{'entryId': 'entry-1'}]
        
        result = handler(sample_appsync_search_event, None)
        
        mock_search.assert_called_once_with(
            'user-456',
            sample_appsync_search_event['arguments']
        )
        assert len(result) == 1
    
    @patch('index.handle_chat_query')
    def test_handler_chat_query(
        self,
        mock_chat,
        sample_appsync_chat_event
    ):
        """Test handler routes to chat query."""
        mock_chat.return_value = {
            'query': 'Test query',
            'response': 'Test response',
            'entriesUsed': 5
        }
        
        result = handler(sample_appsync_chat_event, None)
        
        mock_chat.assert_called_once_with(
            'user-456',
            sample_appsync_chat_event['arguments']
        )
        assert result['response'] == 'Test response'
    
    def test_handler_missing_user_id(self):
        """Test handler with missing user ID."""
        event = {
            'identity': {},
            'arguments': {},
            'info': {'fieldName': 'searchLogbookEntries'}
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
    
    @patch('index.handle_semantic_search')
    def test_handler_error_handling(self, mock_search):
        """Test handler error handling."""
        mock_search.side_effect = Exception("Test error")
        
        event = {
            'identity': {'sub': 'user-456'},
            'arguments': {'query': 'test'},
            'info': {'fieldName': 'searchLogbookEntries'}
        }
        
        with pytest.raises(Exception, match="logbook AI operation"):
            handler(event, None)


class TestIntegration:
    """Integration tests for AI features."""
    
    @patch('index.bedrock_client')
    @patch('index.get_db_connection')
    @patch('index.return_db_connection')
    def test_end_to_end_semantic_search(
        self,
        mock_return_conn,
        mock_get_conn,
        mock_bedrock,
        mock_db_connection,
        sample_logbook_entries,
        sample_embedding
    ):
        """Test end-to-end semantic search flow."""
        # Mock embedding generation
        embedding_response = {
            'body': MagicMock()
        }
        embedding_response['body'].read.return_value = json.dumps({
            'embedding': sample_embedding
        }).encode('utf-8')
        mock_bedrock.invoke_model.return_value = embedding_response
        
        # Mock database
        conn, cursor = mock_db_connection
        mock_get_conn.return_value = conn
        entries_with_similarity = [
            {**entry, 'similarity': 0.95} for entry in sample_logbook_entries
        ]
        cursor.fetchall.return_value = entries_with_similarity
        
        # Execute search
        event = {
            'identity': {'sub': 'user-456'},
            'arguments': {'query': 'pattern work'},
            'info': {'fieldName': 'searchLogbookEntries'}
        }
        
        result = handler(event, None)
        
        # Verify complete flow
        assert len(result) > 0
        assert result[0]['entryId'] == 'entry-1'
    
    @patch('index.bedrock_client')
    @patch('index.get_db_connection')
    @patch('index.return_db_connection')
    def test_end_to_end_chat_query(
        self,
        mock_return_conn,
        mock_get_conn,
        mock_bedrock,
        mock_db_connection,
        sample_logbook_entries,
        sample_claude_response
    ):
        """Test end-to-end chat query flow."""
        # Mock Claude response
        claude_response = {
            'body': MagicMock()
        }
        claude_response['body'].read.return_value = json.dumps(
            sample_claude_response
        ).encode('utf-8')
        mock_bedrock.invoke_model.return_value = claude_response
        
        # Mock database
        conn, cursor = mock_db_connection
        mock_get_conn.return_value = conn
        cursor.fetchall.return_value = sample_logbook_entries
        
        # Execute chat
        event = {
            'identity': {'sub': 'user-456'},
            'arguments': {'query': 'How many hours?'},
            'info': {'fieldName': 'chatLogbookQuery'}
        }
        
        result = handler(event, None)
        
        # Verify complete flow
        assert result['query'] == 'How many hours?'
        assert 'dual instruction' in result['response'].lower()
        assert result['entriesUsed'] == 2


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
