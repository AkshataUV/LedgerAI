"""
services/merchant_grouping.py
─────────────────────────────
Pre-pipeline background grouping job.

Called after uncategorized_transactions are inserted for a document.
Runs fully in a background thread (using the thread-local Supabase client
that process_document already sets up).  The route can also spawn a
dedicated thread for the approve endpoint — see document_routes.py.

Pipeline steps
──────────────
1. Classify each transaction with routing_rules  → pre_pipeline_strategy
2. Embed VECTOR_SEARCH / NO_RULE transactions via ML service /embed
3. Within-batch cosine grouping (threshold 0.92)
4. Cross-document cosine grouping against last 90 days (max 800 rows)
5. Auto-categorise from personal history via match_personal_vectors RPC
6. Mark grouping_status = 'done' on all rows + the document row
"""
from __future__ import annotations

import os
import re
import uuid
import math
import logging
import httpx

from db.connection import get_client

logger = logging.getLogger("ledgerai.merchant_grouping")

# ── Configuration ─────────────────────────────────────────────────────────────
COSINE_THRESHOLD_GROUP  = 0.92
COSINE_THRESHOLD_PVEC   = 0.35
EMBED_BATCH_SIZE   = 10
HISTORY_ROW_LIMIT  = 800
HISTORY_DAYS       = 90
P_VEC_THRESHOLD    = 0.35

# ── ML Service URL ────────────────────────────────────────────────────────────
# Resolved lazily at call-time (not import-time) so Render env vars that are
# injected after module import are picked up correctly.
# Priority: ML_SERVICE_URL env var > http://127.0.0.1:<PYTHON_PORT>
#
# ⚠️  DEPLOYMENT CHECKLIST
#   Render (parser_backend) MUST have ML_SERVICE_URL set to the HuggingFace
#   Space URL, e.g.  https://<org>-<space>.hf.space
#   It CANNOT share the Node.js backend's ML_SERVICE_URL automatically.
def _get_ml_service_url() -> str:
    url = os.environ.get("ML_SERVICE_URL", "").strip()
    if not url:
        fallback = f"http://127.0.0.1:{os.environ.get('PYTHON_PORT', '5000')}"
        logger.critical(
            "ML_SERVICE_URL is not set! Falling back to %s — "
            "embeddings WILL FAIL on any deployed environment where the ML service "
            "is not running on localhost. Set ML_SERVICE_URL on Render.",
            fallback,
        )
        return fallback
    return url

# ── VPA suffix regex (mirrors Node.js STAGE 2) ────────────────────────────────
_VPA_RE   = re.compile(
    r"@(okicici|okaxis|ybl|okhdfcbank|upi|paytm|oksbi|sbi|axl|ibl|icici)\b",
    re.IGNORECASE,
)
_NOISE_RE = re.compile(
    r"\b(UPI|IMPS|NEFT|RTGS|TXN|POS|ECOM|REF|ATM|TRANSFER)\b",
    re.IGNORECASE,
)


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _cosine_similarity(a: list[float], b: list[float]) -> float:
    """Fast cosine similarity for two equal-length float lists."""
    dot = sum(x * y for x, y in zip(a, b))
    mag_a = math.sqrt(sum(x * x for x in a))
    mag_b = math.sqrt(sum(x * x for x in b))
    if mag_a == 0 or mag_b == 0:
        return 0.0
    return dot / (mag_a * mag_b)


def _derive_clean_string_no_rule(details: str) -> str:
    """
    Fallback clean-string derivation for NO_RULE transactions.

    Mirrors the Node.js STAGE 2 regex cleaner exactly so embeddings
    produced here are comparable to the ones the Node.js pipeline would
    produce for the same transaction.
    """
    text = details

    # 1. Remove VPA suffixes
    text = _VPA_RE.sub(" ", text)

    # 2. Strip non-alphanumeric (keep letters, numbers, spaces)
    text = re.sub(r"[^a-zA-Z0-9\s]", " ", text)

    # 3. Remove numbers with 4+ digits
    text = re.sub(r"\b\d{4,}\b", " ", text)

    # 4. Remove noise words
    text = _NOISE_RE.sub(" ", text)

    # 5. Collapse whitespace + trim
    return re.sub(r"\s+", " ", text).strip() or "UNKNOWN"


def _embed_batch(clean_strings: list[str]) -> list[list[float] | None]:
    """
    Call the ML service /embed endpoint for each string in the batch.
    Returns a list of float-list embeddings (or None on failure) in the
    same order as clean_strings.
    """
    ml_url = _get_ml_service_url()
    embed_endpoint = f"{ml_url}/embed"
    logger.info("[EMBED] Calling ML service: %s (%d strings)", embed_endpoint, len(clean_strings))

    results: list[list[float] | None] = []
    for text in clean_strings:
        try:
            resp = httpx.post(
                embed_endpoint,
                json={"text": text.upper()},
                timeout=30.0,          # increased: HuggingFace spaces can be slow
                follow_redirects=True, # HuggingFace Spaces redirect; httpx doesn't follow by default
                verify=True,           # keep True; set False only if HF cert issues arise
            )
            resp.raise_for_status()
            embedding = resp.json().get("embedding")
            if isinstance(embedding, list):
                results.append(embedding)
            else:
                logger.error(
                    "[EMBED] Unexpected response shape for '%s': %s",
                    text[:60], resp.text[:200]
                )
                results.append(None)
        except httpx.ConnectError as exc:
            logger.critical(
                "[EMBED] Connection REFUSED to %s — is ML_SERVICE_URL correct? "
                "Current value: %s  Error: %s",
                embed_endpoint, ml_url, exc,
            )
            results.append(None)
        except httpx.TimeoutException as exc:
            logger.error("[EMBED] Timeout calling %s for '%s': %s", embed_endpoint, text[:60], exc)
            results.append(None)
        except Exception as exc:
            logger.error("[EMBED] Unexpected error for '%s': %s", text[:60], exc)
            results.append(None)
    return results


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 1 — CLASSIFY VIA ROUTING RULES
# ═══════════════════════════════════════════════════════════════════════════════

def _classify_transactions(
    txns: list[dict],
    routing_rules: list[dict],
) -> list[dict]:
    """
    Apply routing_rules to each transaction's details field.

    Returns the same list annotated with:
      - pre_pipeline_strategy : 'FAST_PATH' | 'EXACT_THEN_DUMP' | 'VECTOR_SEARCH' | 'NO_RULE'
      - clean_string          : merchant string for embedding (VECTOR_SEARCH / NO_RULE only)
    """
    for txn in txns:
        details = txn.get("details") or ""
        matched = False

        for rule in routing_rules:
            pattern      = rule.get("pattern", "")
            strategy_type = rule.get("strategy_type", "")

            try:
                m = re.search(pattern, details, re.IGNORECASE)
            except re.error:
                continue

            if not m:
                continue

            txn["pre_pipeline_strategy"] = strategy_type
            matched = True

            if strategy_type == "VECTOR_SEARCH":
                # Try to extract clean string from first capture group
                try:
                    extracted = m.group(1)
                    txn["clean_string"] = extracted.strip() if extracted else details
                except IndexError:
                    txn["clean_string"] = details

            break  # first matching rule wins

        if not matched:
            txn["pre_pipeline_strategy"] = "NO_RULE"
            txn["clean_string"] = _derive_clean_string_no_rule(details)

    return txns


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 3 — WITHIN-BATCH GROUPING
# ═══════════════════════════════════════════════════════════════════════════════

def _group_within_batch(embed_txns: list[dict]) -> list[dict]:
    """
    Pairwise cosine comparison within the current document's transactions.

    - Two transactions are in the same group iff similarity >= COSINE_THRESHOLD_GROUP.
    - Group representative = row with the lowest uncategorized_transaction_id.
    - Every transaction gets a group_id (even singletons).

    Uses Union-Find for O(n α(n)) grouping.
    """
    n = len(embed_txns)
    parent = list(range(n))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x, y):
        rx, ry = find(x), find(y)
        if rx != ry:
            # Smaller id → canonical root
            if embed_txns[rx]["uncategorized_transaction_id"] <= embed_txns[ry]["uncategorized_transaction_id"]:
                parent[ry] = rx
            else:
                parent[rx] = ry

    # O(n²) pairwise — fine for typical document sizes
    for i in range(n):
        for j in range(i + 1, n):
            emb_i = embed_txns[i].get("embedding")
            emb_j = embed_txns[j].get("embedding")
            if emb_i is None or emb_j is None:
                continue
            sim = _cosine_similarity(emb_i, emb_j)
            if sim >= COSINE_THRESHOLD_GROUP:
                union(i, j)

    # Assign UUIDs: all members of a component share the root's UUID
    group_uuids: dict[int, str] = {}
    for i in range(n):
        root = find(i)
        if root not in group_uuids:
            group_uuids[root] = str(uuid.uuid4())
        embed_txns[i]["group_id"] = group_uuids[root]

    return embed_txns


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def run_merchant_grouping(document_id: int, user_id: str) -> None:
    """
    Execute the full 6-step pre-pipeline background grouping job.

    Must be called from a thread that has already called set_thread_client()
    (i.e., from the existing run_processing thread, or from a dedicated thread
    spawned by the approve endpoint with its own make_client() + set_thread_client()).
    """
    sb = get_client()
    logger.info("═" * 60)
    logger.info("MERCHANT GROUPING START — document_id=%s", document_id)
    logger.info("═" * 60)

    # ──────────────────────────────────────────────────────────────────────────
    # FETCH uncategorized_transactions for this document
    # ──────────────────────────────────────────────────────────────────────────
    txn_result = (
        sb.table("uncategorized_transactions")
        .select(
            "uncategorized_transaction_id, details, user_id, document_id, "
            "txn_date, debit, credit, account_id"
        )
        .eq("document_id", document_id)
        .eq("user_id", user_id)
        .execute()
    )
    txns: list[dict] = txn_result.data or []
    if not txns:
        logger.info("No transactions found for document_id=%s — skipping grouping", document_id)
        sb.table("documents").update({"grouping_status": "done"}).eq("document_id", document_id).execute()
        return

    logger.info("Fetched %d transactions for document_id=%s", len(txns), document_id)

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 1 — CLASSIFY using routing_rules
    # ──────────────────────────────────────────────────────────────────────────
    logger.info("[STEP 1] Fetching routing_rules...")
    rules_result = sb.table("routing_rules").select("pattern, strategy_type").execute()
    routing_rules: list[dict] = rules_result.data or []
    logger.info("  %d routing rules loaded", len(routing_rules))

    txns = _classify_transactions(txns, routing_rules)

    # Bulk-update pre_pipeline_strategy + grouping_status for FAST_PATH / EXACT_THEN_DUMP
    fast_ids    = [t["uncategorized_transaction_id"] for t in txns if t["pre_pipeline_strategy"] in ("FAST_PATH", "EXACT_THEN_DUMP")]
    other_ids   = [t["uncategorized_transaction_id"] for t in txns if t["pre_pipeline_strategy"] not in ("FAST_PATH", "EXACT_THEN_DUMP")]

    strategy_groups = {
        "FAST_PATH":       [t for t in txns if t["pre_pipeline_strategy"] == "FAST_PATH"],
        "EXACT_THEN_DUMP": [t for t in txns if t["pre_pipeline_strategy"] == "EXACT_THEN_DUMP"],
        "VECTOR_SEARCH":   [t for t in txns if t["pre_pipeline_strategy"] == "VECTOR_SEARCH"],
        "NO_RULE":         [t for t in txns if t["pre_pipeline_strategy"] == "NO_RULE"],
    }

    for strategy, group in strategy_groups.items():
        if not group:
            continue
        ids = [t["uncategorized_transaction_id"] for t in group]
        update_payload = {"pre_pipeline_strategy": strategy}
        if strategy in ("FAST_PATH", "EXACT_THEN_DUMP"):
            update_payload["grouping_status"] = "skipped"
        (
            sb.table("uncategorized_transactions")
            .update(update_payload)
            .in_("uncategorized_transaction_id", ids)
            .execute()
        )

    logger.info(
        "  Strategy breakdown: FAST_PATH=%d  EXACT_THEN_DUMP=%d  VECTOR_SEARCH=%d  NO_RULE=%d",
        len(strategy_groups["FAST_PATH"]),
        len(strategy_groups["EXACT_THEN_DUMP"]),
        len(strategy_groups["VECTOR_SEARCH"]),
        len(strategy_groups["NO_RULE"]),
    )

    # Transactions that need embedding
    embed_txns = strategy_groups["VECTOR_SEARCH"] + strategy_groups["NO_RULE"]
    if not embed_txns:
        logger.info("No transactions require embedding — marking grouping complete")
        _mark_complete(sb, document_id, txns)
        _trigger_auto_pipeline(document_id, user_id)
        return

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 2 — EMBED in small batches
    # ──────────────────────────────────────────────────────────────────────────
    logger.info("[STEP 2] Embedding %d transactions in batches of %d...", len(embed_txns), EMBED_BATCH_SIZE)

    all_clean_strings = [t["clean_string"] for t in embed_txns]
    embeddings: list[list[float] | None] = []

    for batch_start in range(0, len(all_clean_strings), EMBED_BATCH_SIZE):
        batch = all_clean_strings[batch_start : batch_start + EMBED_BATCH_SIZE]
        embeddings.extend(_embed_batch(batch))

    # Attach embeddings back to transactions and persist
    for txn, emb in zip(embed_txns, embeddings):
        txn["embedding"] = emb
        if emb is not None:
            cache_row = {
                "user_id": user_id,
                "clean_name": txn.get("clean_string", "UNKNOWN").upper(),
                "embedding": emb,
                "status": "staging",
                "hit_count": 0,
            }
            try:
                result = sb.table("personal_vector_cache").upsert(
                    cache_row,
                    on_conflict="user_id,clean_name"
                ).execute()
                if result.data:
                    cache_id = result.data[0].get("cache_id")
                    if cache_id:
                        txn["vector_cache_ref"] = cache_id
                        sb.table("uncategorized_transactions")\
                          .update({"vector_cache_ref": cache_id})\
                          .eq("uncategorized_transaction_id", txn["uncategorized_transaction_id"])\
                          .execute()
            except Exception as exc:
                logger.error("Failed to upsert staging cache for '%s': %s", txn.get("clean_string", "")[:60], exc)

    embedded_txns = [t for t in embed_txns if t.get("embedding") is not None]
    logger.info("  %d / %d transactions embedded successfully", len(embedded_txns), len(embed_txns))

    if not embedded_txns:
        # All embedding calls failed — most likely ML_SERVICE_URL is misconfigured.
        # Mark done anyway so the pipeline doesn't hang, but log loudly.
        logger.critical(
            "[STEP 2] ZERO embeddings succeeded for document_id=%s. "
            "Check that ML_SERVICE_URL is correctly set on this service (Render). "
            "Current ML_SERVICE_URL resolves to: %s",
            document_id, _get_ml_service_url(),
        )
        _mark_complete(sb, document_id, txns)
        _trigger_auto_pipeline(document_id, user_id)
        return

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 3 — WITHIN-BATCH GROUPING
    # ──────────────────────────────────────────────────────────────────────────
    logger.info("[STEP 3] Within-batch cosine grouping (threshold=%.2f)...", COSINE_THRESHOLD_GROUP)
    if embedded_txns:
        embedded_txns = _group_within_batch(embedded_txns)
        
        # Persist after Step 3 — before Step 4 can override
        for txn in embedded_txns:
            gid = txn.get("group_id")
            if gid:
                sb.table("uncategorized_transactions")\
                  .update({"group_id": gid})\
                  .eq("uncategorized_transaction_id", txn["uncategorized_transaction_id"])\
                  .execute()

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 4 — CROSS-DOCUMENT GROUPING
    # ──────────────────────────────────────────────────────────────────────────
    logger.info("[STEP 4] Cross-document grouping (last %d days, max %d rows)...", HISTORY_DAYS, HISTORY_ROW_LIMIT)

    from datetime import datetime, timedelta, timezone
    cutoff = (datetime.now(timezone.utc) - timedelta(days=HISTORY_DAYS)).isoformat()

    history_result = (
        sb.table("uncategorized_transactions")
        .select(
            "group_id, document_id, created_at, "
            "personal_vector_cache!vector_cache_ref(embedding, account_id)"
        )
        .eq("user_id", user_id)
        .neq("document_id", document_id)
        .not_.is_("vector_cache_ref", "null")
        .gte("created_at", cutoff)
        .eq("personal_vector_cache.status", "staging")
        .limit(HISTORY_ROW_LIMIT)
        .execute()
    )
    historical_rows: list[dict] = history_result.data or []

    flat_history = []
    for hist in historical_rows:
        cache = hist.get("personal_vector_cache") or {}
        # personal_vector_cache may be returned as a list or a dict depending
        # on whether Supabase treats the FK as to-one or to-many.
        if isinstance(cache, list):
            cache = cache[0] if cache else {}
        hist_emb = cache.get("embedding")
        if isinstance(hist_emb, str):
            import json
            try:
                hist_emb = json.loads(hist_emb)
            except Exception:
                hist_emb = None
        if not isinstance(hist_emb, list):
            continue
        flat_history.append({
            "embedding": hist_emb,
            "account_id": cache.get("account_id"),
            "group_id": hist.get("group_id"),
        })

    logger.info("  Loaded %d historical rows", len(flat_history))


    for txn in embedded_txns:
        txn_emb = txn.get("embedding")
        if isinstance(txn_emb, str):
            import json
            try:
                txn_emb = json.loads(txn_emb)
            except Exception:
                continue
        if not isinstance(txn_emb, list):
            continue
            
        best_sim_group = 0.0
        best_group_id = None
        
        best_sim_pvec = 0.0
        best_pvec_account = None
        
        for hist in flat_history:
            hist_emb = hist.get("embedding")  # already a list[float] — validated on construction
                
            sim = _cosine_similarity(txn_emb, hist_emb)
            
            if sim >= COSINE_THRESHOLD_GROUP and sim > best_sim_group:
                best_sim_group = sim
                best_group_id = hist.get("group_id")
                
            if sim >= COSINE_THRESHOLD_PVEC and sim < COSINE_THRESHOLD_GROUP and sim > best_sim_pvec:
                acc_id = hist.get("account_id")
                if acc_id:
                    best_sim_pvec = sim
                    best_pvec_account = acc_id

        if best_group_id:
            txn["group_id"] = best_group_id
            (
                sb.table("uncategorized_transactions")
                .update({"group_id": best_group_id})
                .eq("uncategorized_transaction_id", txn["uncategorized_transaction_id"])
                .execute()
            )
            
        if best_pvec_account:
            txn["pvec_hint"] = {"account_id": best_pvec_account, "confidence": best_sim_pvec}

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 5 — AUTO-CATEGORISE FROM PERSONAL HISTORY
    # ──────────────────────────────────────────────────────────────────────────
    logger.info("[STEP 5] Auto-categorising via match_personal_vectors...")

    # Group embedded_txns by group_id
    groups: dict[str, list[dict]] = {}
    for txn in embedded_txns:
        gid = txn.get("group_id")
        if gid:
            groups.setdefault(gid, []).append(txn)

    auto_categorised_count = 0

    for gid, members in groups.items():
        # Representative = member with the lowest uncategorized_transaction_id
        representative = min(members, key=lambda t: t["uncategorized_transaction_id"])
        rep_embedding  = representative.get("embedding")
        if rep_embedding is None:
            continue

        try:
            pvec_result = sb.rpc(
                "match_personal_vectors",
                {
                    "p_user_id":        user_id,
                    "query_embedding":  rep_embedding,
                    "match_threshold":  P_VEC_THRESHOLD,
                    "match_count":      1,
                },
            ).execute()
        except Exception as exc:
            logger.warning("match_personal_vectors RPC failed for group %s: %s", gid, exc)
            pvec_result = None

        pvec_data = pvec_result.data if pvec_result else []
        account_id = None
        conf_score = 0.0

        if pvec_data:
            match      = pvec_data[0]
            account_id = match.get("account_id")
            conf_score = match.get("similarity") or match.get("confidence_score") or 0.0

        if not account_id:
            hint = representative.get("pvec_hint")
            if hint:
                account_id = hint.get("account_id")
                conf_score = hint.get("confidence", 0.0)

        if not account_id:
            continue

        # Write to transactions table for ALL group members
        insert_rows = []
        for member in members:
            amount = member.get("debit") or member.get("credit") or 0
            txn_type = "DEBIT" if member.get("debit") else "CREDIT"
            insert_rows.append({
                "user_id":                      user_id,
                "base_account_id":              member.get("account_id"),
                "offset_account_id":            account_id,
                "document_id":                  document_id,
                "transaction_date":             member.get("txn_date"),
                "details":                      member.get("details"),
                "amount":                       amount,
                "transaction_type":             txn_type,
                "posting_status":               "DRAFT",
                "review_status":                "PENDING",
                "categorised_by":               "P_VEC",
                "confidence_score":             conf_score,
                "attention_level":              "LOW",
                "uncategorized_transaction_id": member["uncategorized_transaction_id"],
            })

        try:
            sb.table("transactions").insert(insert_rows).execute()
            auto_categorised_count += len(insert_rows)
            logger.info(
                "  Auto-categorised group %s (%d txns) → account_id=%s  conf=%.3f",
                gid, len(insert_rows), account_id, conf_score,
            )
        except Exception as exc:
            logger.error("Failed to insert auto-categorised rows for group %s: %s", gid, exc)

    logger.info("  Auto-categorised %d transactions total", auto_categorised_count)

    # CHANGE 4: EXACT_THEN_DUMP miss handling
    exact_dump_txns = strategy_groups.get("EXACT_THEN_DUMP", [])
    if exact_dump_txns:
        # Fetch user's fallback Uncategorised accounts (same lookup as autoPipelineController)
        fallback_result = (
            sb.table("accounts")
            .select("account_id, account_name")
            .eq("user_id", user_id)
            .eq("is_system_generated", True)
            .in_("account_name", ["Uncategorised Expense", "Uncategorised Income"])
            .execute()
        )
        fallback_accounts = fallback_result.data or []
        uncategorised_expense_id = next(
            (a["account_id"] for a in fallback_accounts if a["account_name"] == "Uncategorised Expense"), None
        )
        uncategorised_income_id = next(
            (a["account_id"] for a in fallback_accounts if a["account_name"] == "Uncategorised Income"), None
        )
        if not uncategorised_expense_id or not uncategorised_income_id:
            logger.warning(
                "Could not find Uncategorised Expense/Income accounts for user %s — EXACT_THEN_DUMP rows will have null offset_account_id",
                user_id
            )

        dump_insert_rows = []
        for txn in exact_dump_txns:
            amount = txn.get("debit") or txn.get("credit") or 0
            txn_type = "DEBIT" if txn.get("debit") else "CREDIT"
            fallback_account_id = (
                uncategorised_expense_id if txn_type == "DEBIT" else uncategorised_income_id
            )
            dump_insert_rows.append({
                "user_id":                      user_id,
                "base_account_id":              txn.get("account_id"),
                "offset_account_id":            fallback_account_id,
                "document_id":                  document_id,
                "transaction_date":             txn.get("txn_date"),
                "details":                      txn.get("details"),
                "amount":                       amount,
                "transaction_type":             txn_type,
                "posting_status":               "DRAFT",
                "review_status":                "PENDING",
                "categorised_by":               "UNCATEGORISED",
                "confidence_score":             0.0,
                "attention_level":              "HIGH",
                "is_uncategorised":             True,
                "uncategorized_transaction_id": txn["uncategorized_transaction_id"],
            })
        if dump_insert_rows:
            try:
                sb.table("transactions").insert(dump_insert_rows).execute()
                logger.info("  Dumped %d EXACT_THEN_DUMP transactions to review queue", len(dump_insert_rows))
            except Exception as exc:
                logger.error("Failed to insert EXACT_THEN_DUMP rows: %s", exc)

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 6 — MARK COMPLETE
    # ──────────────────────────────────────────────────────────────────────────
    _mark_complete(sb, document_id, txns)

    # ──────────────────────────────────────────────────────────────────────────
    # STEP 7 — TRIGGER AUTO-PIPELINE (Node.js Stages 1–3)
    # Fire-and-check: log response but never raise. Grouping is already done.
    # ──────────────────────────────────────────────────────────────────────────
    _trigger_auto_pipeline(document_id, user_id)



def _trigger_auto_pipeline(document_id: int, user_id: str) -> None:
    """
    POST to the Node.js /internal/auto-pipeline endpoint after grouping
    completes. This triggers Stages 1-3 of the categorisation pipeline
    (Rules → P_EXACT → Vector) synchronously before marking grouping done
    from the Node side's perspective.

    Fire-and-check: logs the response but does NOT raise. The grouping job
    is already complete at this point regardless of the trigger's outcome.
    """
    node_url = os.environ.get("NODE_BACKEND_URL", "http://127.0.0.1:3000")
    secret = os.environ.get("INTERNAL_SECRET", "")

    if not secret:
        logger.error("[AUTO-PIPELINE] INTERNAL_SECRET is not set — cannot trigger auto-pipeline. Set it in parser_backend/.env")
        return

    try:
        resp = httpx.post(
            f"{node_url}/internal/auto-pipeline",
            json={"document_id": document_id, "user_id": user_id},
            headers={"Authorization": f"Bearer {secret}"},
            timeout=120.0,  # auto-pipeline can take time for large batches
        )
        logger.info(
            "[AUTO-PIPELINE] Triggered — status=%s response=%s",
            resp.status_code,
            resp.text[:200],
        )
    except Exception as exc:
        logger.error("[AUTO-PIPELINE] Failed to trigger: %s", exc)


def _mark_complete(sb, document_id: int, all_txns: list[dict]) -> None:
    """Set grouping_status = 'done' on all uncategorized_transactions and the document row."""
    all_ids = [t["uncategorized_transaction_id"] for t in all_txns]
    if all_ids:
        (
            sb.table("uncategorized_transactions")
            .update({"grouping_status": "done"})
            .in_("uncategorized_transaction_id", all_ids)
            .execute()
        )
    sb.table("documents").update({"grouping_status": "done"}).eq("document_id", document_id).execute()
    logger.info("[STEP 6] Grouping marked done for document_id=%s", document_id)
    logger.info("═" * 60)
    logger.info("MERCHANT GROUPING COMPLETE — document_id=%s", document_id)
    logger.info("═" * 60)
