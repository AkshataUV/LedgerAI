"""
services/code_sandbox.py
────────────────────────
Safe execution of LLM-generated Python extraction code.

Security: AST validation before exec() — blocks import,
open, eval, exec, os, sys, subprocess usage.
"""

import ast
import re
import json
import math
import string
import decimal
import collections
import collections.abc
import itertools
import functools
import typing
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime, date, timedelta

logger = logging.getLogger("ledgerai.code_sandbox")

# Standalone function calls that are dangerous
# (eval, exec, compile compile CODE, open opens files, __import__ imports anything)
BLOCKED_BUILTINS = {"open", "eval", "exec", "compile", "__import__"}

# Modules that must never be accessed (even as attributes)
BLOCKED_MODULES = {"subprocess", "os", "sys", "shutil", "pathlib"}

# Modules the LLM is allowed to import (safe, pure-computation only)
ALLOWED_IMPORTS = {
    "re",
    "math",
    "json",
    "string",
    "datetime",
    "decimal",
    "collections",
    "collections.abc",
    "itertools",
    "functools",
    "typing",
}


def validate_code(code: str) -> Optional[str]:
    """
    Returns error string if code is dangerous or invalid,
    None if safe.
    """
    try:
        tree = ast.parse(code)
    except SyntaxError as e:
        return f"SyntaxError: {e}"

    for node in ast.walk(tree):
        # Block dangerous imports (but allow re, math, etc.)
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name not in ALLOWED_IMPORTS:
                    return f"Blocked: import '{alias.name}' at line {node.lineno}"
        if isinstance(node, ast.ImportFrom):
            if node.module not in ALLOWED_IMPORTS:
                return f"Blocked: from '{node.module}' import at line {node.lineno}"

        # Block dangerous standalone calls: eval(), exec(), compile(), open()
        # But NOT re.compile() — that's a safe method on an allowed module
        if isinstance(node, ast.Name) and node.id in BLOCKED_BUILTINS:
            return f"Blocked: use of '{node.id}' at line {node.lineno}"

        # Block dangerous module access: os.system, subprocess.run, etc.
        if isinstance(node, ast.Attribute) and node.attr in BLOCKED_MODULES:
            return f"Blocked: attribute '{node.attr}' at line {node.lineno}"

    return None


def clean_llm_code(raw: str) -> str:
    """Strip markdown fences and 'python' language tag from LLM output."""
    cleaned = raw.strip()
    if "```" in cleaned:
        parts = cleaned.split("```")
        cleaned = parts[1] if len(parts) > 1 else parts[0]
    cleaned = cleaned.strip()
    if cleaned.lower().startswith("python"):
        cleaned = cleaned[6:].strip()
    return cleaned


def execute_extraction_code(code: str, full_text: str) -> List[Dict]:
    """
    Safely execute LLM-generated extraction code and return
    the result of extract_transactions(text).
    """
    cleaned = clean_llm_code(code)

    # Validate AST before executing
    error = validate_code(cleaned)
    if error:
        raise RuntimeError(f"Code validation failed: {error}")

    namespace = {
        # stdlib modules
        "re": re,
        "json": json,
        "math": math,
        "string": string,
        "decimal": decimal,
        "collections": collections,
        "itertools": itertools,
        "functools": functools,
        "typing": typing,
        # datetime helpers
        "datetime": datetime,
        "date": date,
        "timedelta": timedelta,
        # common typing aliases
        "List": List,
        "Dict": Dict,
        "Optional": Optional,
        "Any": Any,
    }

    exec(cleaned, namespace)

    if "extract_transactions" not in namespace:
        raise ValueError("extract_transactions() not found in generated code.")

    fn = namespace["extract_transactions"]
    result = fn(full_text)

    if not isinstance(result, list):
        raise ValueError(f"Expected list, got {type(result).__name__}")

    return result