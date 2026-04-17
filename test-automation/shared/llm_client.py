"""Azure AI Foundry client wrapper for all agents.

Security: API key retrieved from Key Vault or environment variable.
Never falls back to a placeholder -- fails fast if not configured.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time

from openai import OpenAI

from .config import AppConfig

logger = logging.getLogger(__name__)

MAX_LLM_RETRIES = 3
LLM_RETRY_DELAY_SEC = 2
MAX_PROMPT_CHARS = 100_000


class LLMError(Exception):
    """Raised when LLM calls fail after all retries."""


class LLMClient:
    """Wrapper around Azure AI Foundry endpoint using OpenAI-compatible API."""

    def __init__(self, config: AppConfig):
        api_key = os.environ.get("AI_API_KEY")
        if not api_key:
            try:
                from azure.identity import DefaultAzureCredential
                from azure.keyvault.secrets import SecretClient
                credential = DefaultAzureCredential()
                kv_client = SecretClient(vault_url=config.key_vault_uri, credential=credential)
                api_key = kv_client.get_secret("ai-api-key").value
            except Exception:
                raise LLMError(
                    "AI_API_KEY not set and Key Vault retrieval failed. "
                    "Set AI_API_KEY env var or ensure Key Vault access is configured."
                )

        if not api_key or api_key == "placeholder":
            raise LLMError("AI_API_KEY is not configured. Cannot initialize LLM client.")

        endpoint = config.openai_endpoint.rstrip("/")
        if not endpoint.endswith("/openai/v1"):
            endpoint = endpoint + "/openai/v1"
        logger.info("LLM client init: base_url=%s, deployment=%s", endpoint, config.openai_deployment)

        self._client = OpenAI(
            base_url=endpoint,
            api_key=api_key,
        )
        self._deployment = config.openai_deployment
        self._total_prompt_tokens = 0
        self._total_completion_tokens = 0
        self._total_calls = 0

    @property
    def usage_stats(self) -> dict:
        return {
            "total_calls": self._total_calls,
            "total_prompt_tokens": self._total_prompt_tokens,
            "total_completion_tokens": self._total_completion_tokens,
        }

    def chat(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.0,
        max_tokens: int = 4096,
        response_format: dict | None = None,
    ) -> str:
        """Send a chat completion request and return the response text."""
        # Guard: truncate if prompt is excessively long
        if len(system_prompt) + len(user_prompt) > MAX_PROMPT_CHARS:
            logger.warning(
                "Prompt too large (%d chars), truncating to %d",
                len(system_prompt) + len(user_prompt), MAX_PROMPT_CHARS,
            )
            available = MAX_PROMPT_CHARS - len(system_prompt)
            user_prompt = user_prompt[:max(available, 1000)]

        kwargs: dict = {
            "model": self._deployment,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        if response_format:
            kwargs["response_format"] = response_format

        last_error = None
        for attempt in range(1, MAX_LLM_RETRIES + 1):
            start = time.time()
            try:
                logger.info("LLM call attempt %d/%d (deployment=%s, max_tokens=%d)",
                            attempt, MAX_LLM_RETRIES, self._deployment, max_tokens)
                response = self._client.chat.completions.create(**kwargs, timeout=90)
                elapsed = round(time.time() - start, 1)
                content = response.choices[0].message.content or ""

                prompt_tokens = getattr(response.usage, "prompt_tokens", 0)
                completion_tokens = getattr(response.usage, "completion_tokens", 0)
                self._total_prompt_tokens += prompt_tokens
                self._total_completion_tokens += completion_tokens
                self._total_calls += 1

                logger.info(
                    "LLM call done in %ss: prompt_tokens=%d, completion_tokens=%d, total_calls=%d",
                    elapsed, prompt_tokens, completion_tokens, self._total_calls,
                )
                return content
            except Exception as e:
                elapsed = round(time.time() - start, 1)
                last_error = e
                err_str = str(e).lower()
                is_retryable = any(t in err_str for t in [
                    "rate limit", "429", "timeout", "503", "capacity",
                    "overloaded", "connection", "reset",
                ])
                logger.warning("LLM error after %ss (attempt %d/%d, retryable=%s): %s",
                               elapsed, attempt, MAX_LLM_RETRIES, is_retryable, str(e)[:200])
                if attempt < MAX_LLM_RETRIES and is_retryable:
                    delay = LLM_RETRY_DELAY_SEC * (2 ** (attempt - 1))
                    logger.info("Retrying in %ds...", delay)
                    time.sleep(delay)
                elif not is_retryable:
                    raise LLMError(f"LLM call failed (non-retryable): {str(e)[:300]}") from e

        raise LLMError(f"LLM call failed after {MAX_LLM_RETRIES} attempts: {str(last_error)[:300]}")

    def chat_json(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.0,
        max_tokens: int = 4096,
    ) -> dict:
        """Send a chat request expecting JSON output.

        Handles malformed JSON from LLM with best-effort extraction.
        """
        raw = self.chat(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            temperature=temperature,
            max_tokens=max_tokens,
            response_format={"type": "json_object"},
        )

        # Try direct parse first
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            pass

        # Try extracting JSON from markdown code blocks
        json_match = re.search(r"```(?:json)?\s*\n?(.*?)\n?```", raw, re.DOTALL)
        if json_match:
            try:
                return json.loads(json_match.group(1))
            except json.JSONDecodeError:
                pass

        # Try finding first { ... } block
        brace_match = re.search(r"\{.*\}", raw, re.DOTALL)
        if brace_match:
            try:
                return json.loads(brace_match.group(0))
            except json.JSONDecodeError:
                pass

        logger.error("Failed to parse LLM JSON response (length=%d): %.200s", len(raw), raw)
        raise LLMError(f"LLM returned invalid JSON (length={len(raw)}). First 200 chars: {raw[:200]}")
