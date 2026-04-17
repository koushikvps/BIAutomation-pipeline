"""Tests for LLMClient."""

import json
import pytest
from unittest.mock import patch, MagicMock

from shared.llm_client import LLMClient, LLMError


class TestLLMClient:
    def test_init_fails_without_api_key(self, mock_config, monkeypatch):
        monkeypatch.delenv("AI_API_KEY", raising=False)
        with pytest.raises(LLMError, match="AI_API_KEY"):
            LLMClient(mock_config)

    def test_init_fails_with_placeholder_key(self, mock_config, monkeypatch):
        monkeypatch.setenv("AI_API_KEY", "placeholder")
        with pytest.raises(LLMError, match="not configured"):
            LLMClient(mock_config)

    @patch("shared.llm_client.OpenAI")
    def test_init_success(self, mock_openai_cls, mock_config):
        client = LLMClient(mock_config)
        assert client._deployment == "test-model"
        mock_openai_cls.assert_called_once()

    @patch("shared.llm_client.OpenAI")
    def test_chat_success(self, mock_openai_cls, mock_config):
        mock_response = MagicMock()
        mock_response.choices = [MagicMock(message=MagicMock(content="Hello world"))]
        mock_response.usage = MagicMock(prompt_tokens=10, completion_tokens=5)
        mock_openai_cls.return_value.chat.completions.create.return_value = mock_response

        client = LLMClient(mock_config)
        result = client.chat("system", "user")
        assert result == "Hello world"
        assert client.usage_stats["total_calls"] == 1
        assert client.usage_stats["total_prompt_tokens"] == 10

    @patch("shared.llm_client.OpenAI")
    def test_chat_json_success(self, mock_openai_cls, mock_config):
        mock_response = MagicMock()
        mock_response.choices = [MagicMock(message=MagicMock(content='{"key": "value"}'))]
        mock_response.usage = MagicMock(prompt_tokens=10, completion_tokens=5)
        mock_openai_cls.return_value.chat.completions.create.return_value = mock_response

        client = LLMClient(mock_config)
        result = client.chat_json("system", "user")
        assert result == {"key": "value"}

    @patch("shared.llm_client.OpenAI")
    def test_chat_json_extracts_from_markdown(self, mock_openai_cls, mock_config):
        mock_response = MagicMock()
        mock_response.choices = [MagicMock(message=MagicMock(
            content='Here is the JSON:\n```json\n{"key": "value"}\n```'
        ))]
        mock_response.usage = MagicMock(prompt_tokens=10, completion_tokens=5)
        mock_openai_cls.return_value.chat.completions.create.return_value = mock_response

        client = LLMClient(mock_config)
        result = client.chat_json("system", "user")
        assert result == {"key": "value"}

    @patch("shared.llm_client.OpenAI")
    def test_chat_json_extracts_from_brace_block(self, mock_openai_cls, mock_config):
        mock_response = MagicMock()
        mock_response.choices = [MagicMock(message=MagicMock(
            content='Sure! {"answer": 42} is the result.'
        ))]
        mock_response.usage = MagicMock(prompt_tokens=10, completion_tokens=5)
        mock_openai_cls.return_value.chat.completions.create.return_value = mock_response

        client = LLMClient(mock_config)
        result = client.chat_json("system", "user")
        assert result == {"answer": 42}

    @patch("shared.llm_client.OpenAI")
    def test_chat_json_raises_on_garbage(self, mock_openai_cls, mock_config):
        mock_response = MagicMock()
        mock_response.choices = [MagicMock(message=MagicMock(content="not json at all"))]
        mock_response.usage = MagicMock(prompt_tokens=10, completion_tokens=5)
        mock_openai_cls.return_value.chat.completions.create.return_value = mock_response

        client = LLMClient(mock_config)
        with pytest.raises(LLMError, match="invalid JSON"):
            client.chat_json("system", "user")

    @patch("shared.llm_client.OpenAI")
    def test_prompt_truncation(self, mock_openai_cls, mock_config):
        mock_response = MagicMock()
        mock_response.choices = [MagicMock(message=MagicMock(content="ok"))]
        mock_response.usage = MagicMock(prompt_tokens=10, completion_tokens=5)
        mock_openai_cls.return_value.chat.completions.create.return_value = mock_response

        client = LLMClient(mock_config)
        huge_prompt = "x" * 200_000
        result = client.chat("sys", huge_prompt)
        assert result == "ok"

    @patch("shared.llm_client.OpenAI")
    def test_retry_on_rate_limit(self, mock_openai_cls, mock_config):
        rate_error = Exception("429 rate limit exceeded")
        mock_response = MagicMock()
        mock_response.choices = [MagicMock(message=MagicMock(content="ok"))]
        mock_response.usage = MagicMock(prompt_tokens=10, completion_tokens=5)

        mock_openai_cls.return_value.chat.completions.create.side_effect = [
            rate_error, mock_response,
        ]

        client = LLMClient(mock_config)
        result = client.chat("sys", "user")
        assert result == "ok"
        assert mock_openai_cls.return_value.chat.completions.create.call_count == 2

    @patch("shared.llm_client.OpenAI")
    def test_no_retry_on_auth_error(self, mock_openai_cls, mock_config):
        auth_error = Exception("401 unauthorized")
        mock_openai_cls.return_value.chat.completions.create.side_effect = auth_error

        client = LLMClient(mock_config)
        with pytest.raises(LLMError, match="non-retryable"):
            client.chat("sys", "user")

    @patch("shared.llm_client.OpenAI")
    def test_usage_tracking(self, mock_openai_cls, mock_config):
        mock_response = MagicMock()
        mock_response.choices = [MagicMock(message=MagicMock(content="ok"))]
        mock_response.usage = MagicMock(prompt_tokens=50, completion_tokens=25)
        mock_openai_cls.return_value.chat.completions.create.return_value = mock_response

        client = LLMClient(mock_config)
        client.chat("sys", "user")
        client.chat("sys", "user2")

        stats = client.usage_stats
        assert stats["total_calls"] == 2
        assert stats["total_prompt_tokens"] == 100
        assert stats["total_completion_tokens"] == 50
