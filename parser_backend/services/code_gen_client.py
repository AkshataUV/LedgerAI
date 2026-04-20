"""
services/code_gen_client.py
────────────────────────────
Unified client for code generation LLMs.
Supports Anthropic, OpenRouter, and 9router based on .env configuration.
"""

import logging
import requests
from typing import Dict, Any

from config import (
    CODE_GEN_PROVIDER,
    ANTHROPIC_API_KEY, ANTHROPIC_MODEL,
    OPENROUTER_API_KEY, OPENROUTER_MODEL, OPENROUTER_URL,
    NINEROUTER_API_KEY, NINEROUTER_MODEL, NINEROUTER_URL,
    CODE_GEN_MAX_TOKENS, CODE_GEN_TEMPERATURE
)

logger = logging.getLogger("ledgerai.code_gen_client")


class CodeGenClient:
    """
    Unified client for code generation.
    Automatically routes to the correct provider based on CODE_GEN_PROVIDER env var.
    """

    def __init__(self):
        self.provider = CODE_GEN_PROVIDER.lower()
        logger.info(f"Code generation provider: {self.provider}")

        if self.provider == "anthropic":
            self._init_anthropic()
        elif self.provider == "openrouter":
            self._init_openrouter()
        elif self.provider == "9router":
            self._init_9router()
        else:
            raise ValueError(
                f"Invalid CODE_GEN_PROVIDER: {CODE_GEN_PROVIDER}. "
                f"Must be 'anthropic', 'openrouter', or '9router'"
            )

    def _init_anthropic(self):
        """Initialize Anthropic direct API client."""
        if not ANTHROPIC_API_KEY:
            raise ValueError("ANTHROPIC_API_KEY not set in .env")

        try:
            from anthropic import Anthropic
            self.client = Anthropic(api_key=ANTHROPIC_API_KEY)
            self.model = ANTHROPIC_MODEL
            logger.info(f"Anthropic client initialized: {self.model}")
        except ImportError:
            raise ImportError(
                "anthropic package not installed. Run: pip install anthropic"
            )

    def _init_openrouter(self):
        """Initialize OpenRouter client."""
        if not OPENROUTER_API_KEY:
            raise ValueError("OPENROUTER_API_KEY not set in .env")

        self.api_key = OPENROUTER_API_KEY
        self.model = OPENROUTER_MODEL
        self.url = OPENROUTER_URL
        logger.info(f"OpenRouter client initialized: {self.model}")

    def _init_9router(self):
        """Initialize 9router client."""
        if not NINEROUTER_API_KEY:
            raise ValueError("NINEROUTER_API_KEY not set in .env")

        self.api_key = NINEROUTER_API_KEY
        self.model = NINEROUTER_MODEL
        self.url = NINEROUTER_URL
        logger.info(f"9router client initialized: {self.model}")

    def generate(self, prompt: str, max_retries: int = 3, model: str = None) -> str:
        """
        Generate content using the configured provider.
        Returns the generated text content.
        """
        if self.provider == "anthropic":
            return self._generate_anthropic(prompt, max_retries, model)
        else:
            # Both OpenRouter and 9router use OpenAI-compatible API
            return self._generate_openai_compatible(prompt, max_retries, model)

    def _generate_anthropic(self, prompt: str, max_retries: int, model: str = None) -> str:
        """Generate using Anthropic direct API."""
        import time

        target_model = model if model else self.model

        for attempt in range(max_retries + 1):
            try:
                response = self.client.messages.create(
                    model=target_model,
                    max_tokens=CODE_GEN_MAX_TOKENS,
                    temperature=CODE_GEN_TEMPERATURE,
                    messages=[{"role": "user", "content": prompt}]
                )

                # Extract text from response
                content = response.content[0].text
                logger.info(f"Anthropic generation success: {len(content)} chars")
                return content

            except Exception as e:
                error_str = str(e)
                is_rate_limit = "429" in error_str or "rate_limit" in error_str.lower()

                if is_rate_limit and attempt < max_retries:
                    wait = [5, 10, 20][min(attempt, 2)]
                    logger.warning(
                        f"Anthropic rate limited. Retrying in {wait}s... "
                        f"(attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(wait)
                    continue
                else:
                    logger.error(f"Anthropic API error: {e}")
                    raise

    def _generate_openai_compatible(self, prompt: str, max_retries: int, model: str = None) -> str:
        """Generate using OpenAI-compatible API (OpenRouter or 9router)."""
        import time

        target_model = model if model else self.model

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        # Add provider-specific headers
        if self.provider == "openrouter":
            headers["HTTP-Referer"] = "http://localhost:3000"
            headers["X-Title"] = "LedgerAI"

        data = {
            "model": target_model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": CODE_GEN_TEMPERATURE,
            "max_tokens": CODE_GEN_MAX_TOKENS,
            "stream": False  # Disable streaming - we want the full response at once
        }

        for attempt in range(max_retries + 1):
            try:
                response = requests.post(
                    self.url,
                    headers=headers,
                    json=data,
                    timeout=120
                )

                # Check HTTP status
                if response.status_code != 200:
                    # Log the error response body for debugging
                    try:
                        error_body = response.json()
                        logger.error(f"{self.provider} error response: {error_body}")
                    except:
                        logger.error(f"{self.provider} error response (raw): {response.text[:500]}")

                    if response.status_code == 429 and attempt < max_retries:
                        wait = [5, 10, 20][min(attempt, 2)]
                        logger.warning(
                            f"{self.provider} rate limited (429). "
                            f"Retrying in {wait}s... (attempt {attempt + 1}/{max_retries})"
                        )
                        time.sleep(wait)
                        continue
                    response.raise_for_status()

                # Parse response
                try:
                    result = response.json()
                except ValueError as json_err:
                    logger.error(
                        f"{self.provider} returned invalid JSON. "
                        f"Status: {response.status_code}, "
                        f"Body: {response.text[:500]}"
                    )
                    raise ValueError(
                        f"{self.provider} returned invalid JSON response. "
                        f"Status: {response.status_code}"
                    )

                # Check for error in response body
                if "error" in result:
                    err_msg = result["error"].get("message", "Unknown error")
                    err_code = result["error"].get("code")

                    if err_code == 429 and attempt < max_retries:
                        wait = [5, 10, 20][min(attempt, 2)]
                        logger.warning(
                            f"{self.provider} rate limit error. "
                            f"Retrying in {wait}s... (attempt {attempt + 1}/{max_retries})"
                        )
                        time.sleep(wait)
                        continue

                    raise RuntimeError(
                        f"{self.provider} API error: {err_msg} (code={err_code})"
                    )

                # Extract content
                if "choices" not in result or not result["choices"]:
                    raise ValueError(f"{self.provider} response missing 'choices'")

                content = result["choices"][0]["message"]["content"]
                logger.info(f"{self.provider} generation success: {len(content)} chars")
                return content

            except requests.exceptions.RequestException as e:
                if attempt < max_retries:
                    wait = [5, 10, 20][min(attempt, 2)]
                    logger.warning(
                        f"{self.provider} request failed: {e}. "
                        f"Retrying in {wait}s... (attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(wait)
                    continue
                else:
                    logger.error(f"{self.provider} request failed: {e}")
                    raise


# Singleton instance
_client = None

def get_code_gen_client() -> CodeGenClient:
    """Get or create the singleton code generation client."""
    global _client
    if _client is None:
        _client = CodeGenClient()
    return _client
