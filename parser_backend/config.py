# backend/config.py
import os
from pathlib import Path
from dotenv import load_dotenv

# Absolute path to the .env in project root
dotenv_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path)

# ── Supabase credentials ─────────────────────────────────────
SUPABASE_URL              = os.environ.get("SUPABASE_URL", "")
SUPABASE_ANON_KEY         = os.environ.get("SUPABASE_ANON_KEY", "")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

GEMINI_API_KEY    = os.environ.get("GEMINI_API_KEY")
CLASSIFIER_MODEL = os.environ.get("CLASSIFIER_MODEL", "models/gemini-2.5-flash")
# CODE_GEN_MODEL = os.environ.get("CODE_GEN_MODEL", "gemini-3.1-flash-lite-preview")
LLM_PARSER_MODEL = os.environ.get("LLM_PARSER_MODEL", "models/gemini-2.5-flash")


# ── Code Generation LLM (Claude/OpenRouter/9router) ──────────
# Provider: "anthropic", "openrouter", or "9router"
CODE_GEN_PROVIDER = os.environ.get("CODE_GEN_PROVIDER", "anthropic")

# Anthropic Direct API
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
ANTHROPIC_MODEL   = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-5-20241022")

# OpenRouter (supports Claude and other models)
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")
OPENROUTER_MODEL   = os.environ.get("OPENROUTER_MODEL", "anthropic/claude-sonnet-4.5")
OPENROUTER_URL     = "https://openrouter.ai/api/v1/chat/completions"

# 9router (cheaper alternative)
NINEROUTER_API_KEY = os.environ.get("NINEROUTER_API_KEY")
NINEROUTER_MODEL   = os.environ.get("NINEROUTER_MODEL", "anthropic/claude-sonnet-4.5")
NINEROUTER_URL     = os.environ.get("NINEROUTER_URL", "https://api.9router.com/v1/chat/completions")

# Common settings
CODE_GEN_MAX_TOKENS   = int(os.environ.get("CODE_GEN_MAX_TOKENS", "4096"))
CODE_GEN_TEMPERATURE  = float(os.environ.get("CODE_GEN_TEMPERATURE", "0"))