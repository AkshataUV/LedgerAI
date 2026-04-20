"""
services/llm_retry.py
─────────────────────
Retry wrapper for Gemini API calls with exponential backoff.
Handles 429 RESOURCE_EXHAUSTED errors gracefully by reading the
server-suggested retry delay from the error response.
"""

import re
import time
import logging

logger = logging.getLogger("ledgerai.llm_retry")


def _parse_retry_delay(error_str: str) -> float:
    """Extract the retryDelay seconds from the API error message."""
    match = re.search(r'retryDelay.*?(\d+(?:\.\d+)?)\s*s', error_str)
    if match:
        return float(match.group(1))
    # Also try "Please retry in Xs" pattern
    match = re.search(r'retry in\s+(\d+(?:\.\d+)?)\s*s', error_str, re.IGNORECASE)
    if match:
        return float(match.group(1))
    return 0


def call_with_retry(client, model, contents, config, max_retries=3):
    """
    Call Gemini generate_content with retry on 429 errors.
    Uses server-suggested retry delay when available,
    otherwise falls back to exponential backoff: 10s, 30s, 60s.
    """
    fallback_waits = [10, 30, 60]

    for attempt in range(max_retries + 1):
        try:
            response = client.models.generate_content(
                model=model,
                contents=contents,
                config=config,
            )
            return response
        except Exception as e:
            error_str = str(e)
            is_rate_limit = "429" in error_str or "RESOURCE_EXHAUSTED" in error_str

            if is_rate_limit and attempt < max_retries:
                # Try to use the server-suggested delay
                server_delay = _parse_retry_delay(error_str)
                fallback = fallback_waits[min(attempt, len(fallback_waits) - 1)]
                wait = max(server_delay + 2, fallback)  # Add 2s buffer to server delay

                logger.warning(
                    "Rate limited (429). Waiting %.1fs before retry... (attempt %d/%d)",
                    wait, attempt + 1, max_retries,
                )
                time.sleep(wait)
                continue
            else:
                raise
# import time
# import logging
# import requests

# logger = logging.getLogger("ledgerai.llm_retry")


# def call_with_retry(api_key, model, prompt, max_retries=3):
#     url = "https://openrouter.ai/api/v1/chat/completions"

#     headers = {
#     "Authorization": f"Bearer {api_key}",
#     "Content-Type": "application/json",
#     "HTTP-Referer": "http://localhost:3000",  # change in production
#     "X-Title": "ledger-ai"
# }

#     fallback_waits = [5, 10, 20]

#     for attempt in range(max_retries + 1):
#         try:
#             data = {
#                 "model": model,
#                 "messages": [
#                     {"role": "user", "content": prompt}
#                 ],
#                 "temperature": 0,
#                 "max_tokens": 4096
#             }

#             # Added timeout to prevent infinite hangs
#             response = requests.post(url, headers=headers, json=data, timeout=120)

#             # Check for non-200 errors
#             if response.status_code != 200:
#                 # Handle rate limit
#                 if response.status_code == 429 and attempt < max_retries:
#                     wait = fallback_waits[min(attempt, len(fallback_waits) - 1)]
#                     logger.warning(f"Rate limited (429). Retrying in {wait}s...")
#                     time.sleep(wait)
#                     continue
#                 # Other HTTP errors
#                 response.raise_for_status()

#             # For 200 OK, check if OpenRouter returned an error block inside the JSON
#             result = response.json()
#             if "error" in result:
#                 err_msg = result["error"].get("message", "Unknown OpenRouter error")
#                 err_code = result["error"].get("code")
                
#                 # If it's a rate limit error (code 429), retry
#                 if err_code == 429 and attempt < max_retries:
#                     wait = fallback_waits[min(attempt, len(fallback_waits) - 1)]
#                     logger.warning(f"OpenRouter Rate Limit Error (429): {err_msg}. Retrying in {wait}s...")
#                     time.sleep(wait)
#                     continue
                
#                 # If it's another error, raise it
#                 raise RuntimeError(f"OpenRouter API Error: {err_msg} (code={err_code})")

#             # Check if choices is present
#             if "choices" not in result:
#                 raise ValueError(f"OpenRouter response missing 'choices' field: {result}")

#             return result

#         except Exception as e:
#             if attempt < max_retries:
#                 # Retry on connection/timeout issues as well
#                 wait = fallback_waits[min(attempt, len(fallback_waits) - 1)]
#                 logger.warning(f"Attempt {attempt+1} failed: {e}. Retrying in {wait}s...")
#                 time.sleep(wait)
#             else:
#                 raise