import logging
import time
from typing import Union

import requests
from google import genai
from google.genai import types

from config import (
    GEMINI_API_KEY,
    CLASSIFIER_MODEL,
    LLM_PARSER_MODEL,
    OPENROUTER_API_KEY,
    OPENROUTER_URL,
)

logger = logging.getLogger("ledgerai.llm_provider")

# ── Provider config ───────────────────────────────────────────────────────────

# Gemini model names → OpenRouter equivalents
_GEMINI_TO_OPENROUTER = {
    "models/gemini-2.5-flash":        "google/gemini-2.5-flash-preview",
    "models/gemini-2.5-flash-latest": "google/gemini-2.5-flash-preview",
    "models/gemini-2.0-flash":        "google/gemini-2.0-flash-001",
    "models/gemini-1.5-flash":        "google/gemini-1.5-flash",
    "models/gemini-1.5-pro":          "google/gemini-1.5-pro",
}

# Last-resort model when even Gemini-via-OpenRouter fails.
# Claude Haiku is fast, cheap, and great at structured JSON extraction.
_OPENROUTER_FALLBACK_MODEL = "anthropic/claude-haiku-4-5"

# Retry settings for the Gemini direct path
_GEMINI_RETRY_ATTEMPTS = 3
_GEMINI_RETRY_DELAYS   = [2, 5, 10]   # seconds between retries

# ── Gemini client (reuse across calls) ───────────────────────────────────────
_gemini_client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None


# ═════════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ═════════════════════════════════════════════════════════════════════════════

def call_llm(
    prompt:  Union[str, None] = None,
    parts:   Union[list, None] = None,
    model:   str = None,
    temperature: float = 0,
) -> str:
    """
    Call the LLM with automatic fallback.

    Args:
        prompt:      Plain text prompt (use this OR parts, not both).
        parts:       List of google.genai.types.Part objects for multimodal calls.
        model:       Gemini model string (e.g. "models/gemini-2.5-flash").
                     Defaults to LLM_PARSER_MODEL from config.
        temperature: Sampling temperature (default 0 for deterministic extraction).

    Returns:
        Model response as a plain string.

    Raises:
        RuntimeError: If all providers fail.
    """
    if model is None:
        model = LLM_PARSER_MODEL

    content = parts if parts is not None else prompt
    if content is None:
        raise ValueError("Either prompt or parts must be provided")

    errors = []

    # ── 1. Gemini direct ─────────────────────────────────────────────────────
    if _gemini_client:
        for attempt in range(_GEMINI_RETRY_ATTEMPTS):
            try:
                response = _gemini_client.models.generate_content(
                    model=model,
                    contents=content,
                    config=types.GenerateContentConfig(temperature=temperature),
                )
                logger.debug("Gemini direct OK (attempt %d)", attempt + 1)
                return response.text.strip()

            except Exception as e:
                err_str = str(e)
                errors.append(f"gemini_direct[{attempt+1}]: {err_str}")
                logger.warning("Gemini direct attempt %d failed: %s", attempt + 1, err_str)

                if attempt < _GEMINI_RETRY_ATTEMPTS - 1:
                    time.sleep(_GEMINI_RETRY_DELAYS[attempt])
    else:
        logger.warning("Gemini client not initialised (no GEMINI_API_KEY) — skipping")

    # ── 2. Gemini via OpenRouter ──────────────────────────────────────────────
    if OPENROUTER_API_KEY:
        or_model = _GEMINI_TO_OPENROUTER.get(model, "google/gemini-2.5-flash-preview")
        try:
            result = _call_openrouter(
                model=or_model,
                prompt=prompt,
                parts=parts,
                temperature=temperature,
            )
            logger.info("OpenRouter Gemini fallback OK (model=%s)", or_model)
            return result
        except Exception as e:
            errors.append(f"openrouter_gemini: {e}")
            logger.warning("OpenRouter Gemini fallback failed: %s", e)

        # ── 3. Fallback model via OpenRouter ──────────────────────────────────
        try:
            result = _call_openrouter(
                model=_OPENROUTER_FALLBACK_MODEL,
                prompt=prompt,
                parts=parts,
                temperature=temperature,
            )
            logger.info("OpenRouter fallback model OK (model=%s)", _OPENROUTER_FALLBACK_MODEL)
            return result
        except Exception as e:
            errors.append(f"openrouter_fallback: {e}")
            logger.warning("OpenRouter fallback model failed: %s", e)
    else:
        logger.warning("OPENROUTER_API_KEY not set — skipping OpenRouter fallback")

    # ── All providers failed ──────────────────────────────────────────────────
    summary = " | ".join(errors)
    logger.error("All LLM providers failed: %s", summary)
    raise RuntimeError(
        f"All LLM providers failed. This is usually a temporary capacity issue — "
        f"please retry in a few seconds. Details: {summary}"
    )


# ═════════════════════════════════════════════════════════════════════════════
# INTERNAL HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def _call_openrouter(
    model: str,
    prompt: Union[str, None],
    parts: Union[list, None],
    temperature: float,
) -> str:
    """
    Call any OpenRouter model using the OpenAI-compatible REST endpoint.

    For vision/multimodal calls (parts list), we extract the text portions.
    OpenRouter supports base64 images but NOT Google's types.Part objects —
    so PDF bytes are not forwarded (vision falls back to text-only on this path).
    """
    if prompt:
        messages = [{"role": "user", "content": prompt}]
    elif parts:
        # Extract text parts; skip raw bytes (PDF/image data)
        text_parts = []
        for p in parts:
            if isinstance(p, str):
                text_parts.append(p)
            elif isinstance(p, types.Part) and hasattr(p, "text") and p.text:
                text_parts.append(p.text)
        combined = "\n".join(text_parts)
        if not combined.strip():
            raise ValueError("No text content extractable from parts for OpenRouter")
        messages = [{"role": "user", "content": combined}]
    else:
        raise ValueError("No content to send")

    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://ledgerai.app",   # OpenRouter asks for this
        "X-Title": "LedgerAI",
    }
    body = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": 8192,
    }

    resp = requests.post(OPENROUTER_URL, headers=headers, json=body, timeout=120)
    resp.raise_for_status()
    data = resp.json()

    # OpenRouter returns OpenAI-format responses
    return data["choices"][0]["message"]["content"].strip()