import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor

from services.pdf_service import extract_pdf_text
from services.identifier_service import (
    classify_document_llm,
    check_format_exists,
    save_new_statement_format,
)
from services.extraction_service import (
    generate_extraction_logic_llm,
    generate_vetted_extraction_logic,
    vet_and_repair_existing_logic,
    extract_transactions_using_logic,
    refine_extraction_logic_llm,
)
from services.llm_parser import parse_with_llm
from db.connection import get_client, make_client, set_thread_client, clear_thread_client
from services.validation_service import (
    validate_transactions,
    extract_json_from_response,
    validate_extraction_propriety,
    validate_code_quality_strict,       
)
from repository.document_repo import (
    get_document,
    get_document_password,
    update_processing_start,
    update_document_status,
    insert_audit,
    insert_text_extraction,
    update_document_statement,
    update_processing_complete,
    insert_staging_transactions,
    insert_staging_code_only,
    get_text_extraction,
)
from repository.statement_category_repo import (
    activate_statement_category,
    update_statement_status,
    update_success_rate,
    get_statement_by_id,
    update_extraction_logic,
)

logger = logging.getLogger("ledgerai.processing_engine")


# ═══════════════════════════════════════════════════════════
# MAIN PIPELINE
# ═══════════════════════════════════════════════════════════

def process_document(document_id: int, override_file_path: str = None, retry_mode: str = "AUTO", retry_note: str = None):
    """
    Main entry point. Called after document is inserted into DB.
    Runs the complete pipeline: extract → identify → parse → validate → stage.

    override_file_path: if provided, use this local path for PDF extraction
    instead of the file_path stored in the DB. This keeps the DB file_path
    pointing to the permanent Supabase Storage path at all times.
    """

    try:
        # ─────────────────────────────────────────────────────
        # STEP 1 — FETCH DOCUMENT
        # ─────────────────────────────────────────────────────
        logger.info("")
        logger.info("═" * 70)
        logger.info("  PIPELINE START — document_id=%s", document_id)
        logger.info("═" * 70)

        doc = get_document(document_id)
        if not doc:
            raise ValueError(f"Document {document_id} not found.")

        # Use override_file_path (local temp file) if provided,
        # otherwise fall back to the DB path (Supabase Storage path).
        # Never patch the DB file_path — that caused file_path to go NULL.
        file_path = override_file_path or doc["file_path"]
        user_id   = doc["user_id"]
        password  = get_document_password(document_id)

        logger.info("[STEP 1/5] Document fetched")
        logger.info("file      : %s", doc["file_name"])
        logger.info("path      : %s", file_path)
        logger.info("user_id   : %s", user_id)
        logger.info("password  : %s", "YES" if password else "NO")
        logger.info("═" * 70)

        update_processing_start(document_id)
        insert_audit(document_id, "EXTRACTING_TEXT", f"Mode: {retry_mode}")

        # Cleanup old staging rows if this is a retry
        if retry_mode != "AUTO":
             sb = get_client()
             sb.table("ai_transactions_staging").delete().eq("document_id", document_id).execute()

        # ─────────────────────────────────────────────────────
        # STEP 2 — EXTRACT TEXT FROM PDF
        # ─────────────────────────────────────────────────────
        logger.info("")
        logger.info("[STEP 2/5] Extracting text from PDF...")
        update_document_status(document_id, "EXTRACTING_TEXT")

        full_text = None
        if retry_mode != "AUTO":
            full_text = get_text_extraction(document_id)
        
        if full_text:
            logger.info("Found existing text extraction in DB — skipping PDF extraction")
        else:
            full_text = extract_pdf_text(file_path, password)
            if not full_text:
                raise ValueError("PDF extraction returned empty text.")
            insert_text_extraction(document_id, full_text)

        sample_text = full_text[:20000]
        # Split full_text into per-page list
        pages = [
            block.strip()
            for block in re.split(r'={80}', full_text)
            if block.strip() and not re.fullmatch(r'\s*PAGE\s+\d+\s*', block.strip(), re.IGNORECASE)
        ]
        if not pages:
            pages = [full_text]

        logger.info("pages     : %d", len(pages))
        logger.info("chars     : %d", len(full_text))
        logger.info("Text handled successfully")
        logger.info("═" * 70)

        if retry_mode == "MANUAL":
            logger.info("MANUAL mode — skipping automatic extraction")
            update_document_status(document_id, "AWAITING_REVIEW")
            insert_staging_code_only(document_id, user_id, [], 1.0) # Empty placeholder
            insert_audit(document_id, "COMPLETED", "Manual entry requested: " + (retry_note or ""))
            return

        update_document_status(document_id, "IDENTIFYING_FORMAT")

        # ─────────────────────────────────────────────────────
        # STEP 3 — CLASSIFY
        # ─────────────────────────────────────────────────────
        logger.info("")
        logger.info("[STEP 3/5] Identifying statement format...")

        existing = None
        identity_json = None

        # Optimization: check if statement_id is already linked to doc
        if retry_mode != "AUTO" and doc.get("statement_id"):
            existing = get_statement_by_id(doc["statement_id"])
            if existing:
                identity_json = existing.get("statement_identifier", {})
                logger.info("REUSING IDENTIFICATION: using linked statement_id=%s", doc["statement_id"])

        if not existing:
            logger.info("Generating identification markers via LLM...")
            identity_json = classify_document_llm(pages)
            logger.info("Identification generated: %s", identity_json.get("id"))
            logger.info("Checking if format exists in database...")
            existing = check_format_exists(identity_json)

        matched = existing is not None

        if matched:
            logger.info("EXISTING FORMAT DETECTED")
            logger.info("format    : %s", existing.get("format_name", "?"))
            logger.info("statement : %s", existing.get("statement_id"))
            logger.info("status    : %s", existing.get("status"))
        else:
            logger.info("NO MATCHING FORMAT — new format will be saved")
        logger.info("═" * 70)

        update_document_status(document_id, "PARSING_TRANSACTIONS")

        # ═══════════════════════════════════════════════════
        # CASE A — FORMAT EXISTS IN DB
        # ═══════════════════════════════════════════════════
        if matched:
            # Use the stored identifier_json and extraction code from DB
            identity_json    = existing.get("statement_identifier", {})
            extraction_code  = existing["extraction_logic"]
            statement_id     = existing["statement_id"]
            statement_status = existing.get("status")

            update_document_statement(document_id, statement_id)

            # --- LOGIC REFINEMENT (Triggered by Retry Note) ---
            if retry_mode == "CODE" and retry_note and extraction_code:
                try:
                    logger.info("REFINEMENT: Feedback detected for existing format — attempting to improve logic...")
                    refined_code = refine_extraction_logic_llm(
                        current_logic = extraction_code,
                        user_feedback = retry_note,
                        text_sample   = sample_text
                    )
                    if refined_code:
                        logger.info("REFINEMENT: Logic improved successfully. Updating DB.")
                        update_extraction_logic(statement_id, refined_code)
                        extraction_code = refined_code
                except Exception as e:
                    logger.warning("REFINEMENT: Logic improvement failed: %s", e)

            # ── CASE A1 — ACTIVE → Fast path (CODE ONLY, no LLM) ──
            if statement_status == "ACTIVE":
                logger.info("")
                logger.info("[STEP 4/5] ACTIVE format — running stored extraction code (fast path)...")
                logger.info("Skipping LLM (format is trusted)")

                code_txns = extract_transactions_using_logic(full_text, extraction_code)
                logger.info("Code extracted %d transactions", len(code_txns))

                propriety_ok = validate_extraction_propriety(code_txns)
                strict_ok    = validate_code_quality_strict(code_txns)   # BUG-02 fix

                if propriety_ok and strict_ok:
                    logger.info("[STEP 5/5] PIPELINE COMPLETE — CODE (ACTIVE fast-path)")
                    update_processing_complete(document_id, "CODE")
                    insert_staging_code_only(document_id, user_id, code_txns, 100.0)
                    update_document_status(document_id, "AWAITING_REVIEW")
                    insert_audit(document_id, "COMPLETED")
                    logger.info("═" * 70)
                    return  # ← EXIT fast path

                else:
                    logger.warning(
                        "ACTIVE code produced improper transactions "
                        "(propriety=%s strict=%s) — downgrading to UNDER_REVIEW",
                        propriety_ok, strict_ok
                    )
                    update_statement_status(statement_id, "UNDER_REVIEW")
                    # Fall through to dual pipeline below

            # ── CASE A2 — UNDER_REVIEW / EXPERIMENTAL ──
            logger.info("Format status is %s — running vetting gate...", statement_status)
            
            first_page_text = pages[0] if pages else full_text
            extraction_code = vet_and_repair_existing_logic(
                statement_id    = statement_id,
                stored_code     = extraction_code,
                identifier_json = identity_json,
                first_page_text = first_page_text,
                text_sample     = sample_text
            )
            logger.info("Vetting gate complete — continuing to dual pipeline...")

        # ═══════════════════════════════════════════════════
        # CASE B — NEW FORMAT → GENERATE EXTRACTION CODE + SAVE
        # ═══════════════════════════════════════════════════
        else:
            logger.info("")
            logger.info("[STEP 3c/5] Generating VETTED extraction code via LLM loop...")
            
            # Use Page 1 for vetted generation
            first_page_text = pages[0] if pages else full_text
            
            extraction_code = generate_vetted_extraction_logic(
                identifier_json=identity_json,
                first_page_text=first_page_text,
                text_sample=sample_text,
            )
            logger.info("Vetted extraction code finalized (%d chars)", len(extraction_code))

            logger.info("")
            logger.info("[STEP 3d/5] Saving new format to database...")
            statement_id = save_new_statement_format(
                format_name=identity_json.get("id", "AUTO_FORMAT"),
                identifier_json=identity_json,
                extraction_logic=extraction_code,
            )
            update_document_statement(document_id, statement_id)
            statement_status = "UNDER_REVIEW"
            logger.info("Saved as statement_id=%s (UNDER_REVIEW)", statement_id)

        # ═══════════════════════════════════════════════════
        # STEP 4 — TRANSACTIONS EXTRACTION
        # ═══════════════════════════════════════════════════
        logger.info("")
        logger.info("[STEP 4/5] Running Transaction Extraction (mode=%s)...", retry_mode)

        code_txns = []
        llm_txns  = []

        # ── CASE 1: CODE-ONLY RETRY ──
        if retry_mode == "CODE":
            try:
                code_txns = extract_transactions_using_logic(full_text, extraction_code)
                logger.info("CODE extraction complete: %d transactions", len(code_txns))
            except Exception as e:
                logger.error("CODE extraction FAILED: %s", e)
            
            # Outcome: Code always wins if it produced anything
            final_parser_type = "CODE"
            update_processing_complete(document_id, final_parser_type)
            insert_staging_code_only(document_id, user_id, code_txns, 100.0)
            update_document_status(document_id, "AWAITING_REVIEW")
            insert_audit(document_id, "COMPLETED", "Code-only retry")
            logger.info("═" * 70)
            return

        # ── CASE 2: VISION-ONLY RETRY ──
        if retry_mode == "VISION":
            logger.info("VISION mode — skipping dual pipeline")
            # Will be handled by vision block below

        # ── CASE 3: STANDARD DUAL PIPELINE ──
        else:
            with ThreadPoolExecutor(max_workers=2) as executor:
                future_code = executor.submit(
                    extract_transactions_using_logic, full_text, extraction_code
                )
                future_llm = executor.submit(
                    parse_with_llm, full_text, identity_json
                )

                try:
                    llm_response = future_llm.result()
                    llm_txns     = extract_json_from_response(llm_response)
                    logger.info("LLM extraction complete: %d transactions", len(llm_txns))
                except Exception as e:
                    logger.error("LLM extraction FAILED: %s", e)

                try:
                    code_txns = future_code.result()
                    logger.info("CODE extraction complete: %d transactions", len(code_txns))
                except Exception as e:
                    logger.warning("CODE extraction FAILED: %s", e)

        # ── VISION EXTRACTION OVERRIDE ──
        if retry_mode == "VISION":
            logger.info("")
            logger.info("[STEP 4v/5] Running VISION EXTRACTION (Multimodal)...")
            from services.llm_parser import parse_with_vision
            try:
                # Open the file again to send bytes
                with open(file_path, "rb") as f:
                    pdf_bytes = f.read()
                
                vision_response = parse_with_vision(pdf_bytes, identity_json, retry_note)
                llm_txns = extract_json_from_response(vision_response)
                logger.info("VISION extraction complete: %d transactions", len(llm_txns))
                
                # In VISION mode, LLM (vision) always wins unless it fails
                if llm_txns:
                    final_parser_type = "LLM"
                    update_processing_complete(document_id, final_parser_type)
                    insert_staging_transactions(
                        document_id=document_id,
                        user_id=user_id,
                        code_txns=code_txns,
                        llm_txns=llm_txns,
                        code_confidence=0.5,
                        llm_confidence=0.9,
                    )
                    update_document_status(document_id, "AWAITING_REVIEW")
                    insert_audit(document_id, "COMPLETED", "Vision extraction used: " + (retry_note or ""))
                    return
            except Exception as e:
                logger.error("VISION extraction FAILED: %s", e)
                # Fall through to normal decision if vision fails

        logger.info("Results: CODE=%d txns | LLM=%d txns", len(code_txns), len(llm_txns))

        # ═══════════════════════════════════════════════════
        # STEP 5 — VALIDATION & DECISION
        # ═══════════════════════════════════════════════════
        logger.info("")
        logger.info("[STEP 5/5] VALIDATION & ACCURACY CHECK...")

        metrics           = validate_transactions(code_txns, llm_txns)
        comparison_score  = metrics.get("overall_accuracy", 0) if metrics else 0

        code_confidence = round(
            sum(t.get("confidence", 0) for t in code_txns) / len(code_txns), 2
        ) if code_txns else 0

        llm_confidence = round(
            sum(t.get("confidence", 0) for t in llm_txns) / len(llm_txns), 2
        ) if llm_txns else 0

        # BUG-02 fix: require BOTH gates to pass for CODE to win
        code_is_proper = validate_extraction_propriety(code_txns)
        code_is_strict = validate_code_quality_strict(code_txns)
        code_passes_quality = code_is_proper and code_is_strict

        has_code = len(code_txns) > 0
        has_llm  = len(llm_txns) > 0

        logger.info("Code accuracy    : %.2f%%", comparison_score)
        logger.info("Code confidence  : %.2f",   code_confidence)
        logger.info("LLM confidence   : %.2f",   llm_confidence)
        logger.info("Code propriety   : %s",      "PASS" if code_is_proper else "FAIL")
        logger.info("Code strict gate : %s",      "PASS" if code_is_strict else "FAIL")
        logger.info("Has CODE txns    : %s",      has_code)
        logger.info("Has LLM txns     : %s",      has_llm)

        # ── CASE 3: CODE extracted, LLM returned nothing ──────────────────────
        # LLM failed (truncation / timeout / parse error) — do not punish CODE.
        # Status stays EXPERIMENTAL because there is no LLM ground truth to
        # validate against, so we cannot promote to ACTIVE yet.
        if has_code and not has_llm:
            if code_passes_quality:
                final_parser_type    = "CODE"
                new_statement_status = "EXPERIMENTAL"
                logger.info("DECISION: CODE WINS — LLM returned 0 transactions (no ground truth to compare)")
                logger.info("Format status → EXPERIMENTAL (cannot promote to ACTIVE without LLM ground truth)")
            else:
                # CODE extracted something but failed quality gates, LLM is empty —
                # nothing trustworthy from either side. Keep EXPERIMENTAL and use CODE
                # so the user at least sees something rather than blank transactions.
                final_parser_type    = "CODE"
                new_statement_status = "EXPERIMENTAL"
                logger.warning(
                    "DECISION: CODE (quality gates FAILED) — LLM also returned 0 transactions. "
                    "Using CODE as best available output. propriety=%s strict=%s",
                    code_is_proper, code_is_strict,
                )
                logger.info("Format status → EXPERIMENTAL")

        # ── CASE 2: LLM extracted, CODE returned nothing ──────────────────────
        elif has_llm and not has_code:
            final_parser_type    = "LLM"
            new_statement_status = "EXPERIMENTAL"
            logger.info("DECISION: LLM WINS — CODE returned 0 transactions")
            logger.info("Format status → EXPERIMENTAL")

        # ── CASE 1: Both extracted — score-based decision ─────────────────────
        # Promotion to ACTIVE requires high accuracy AND a MATCHING count
        elif comparison_score >= 90 and code_passes_quality and len(code_txns) == len(llm_txns):
            final_parser_type    = "CODE"
            
            # FAST-TRACK: If it's a perfect 100% match, go straight to ACTIVE
            if comparison_score == 100:
                new_statement_status = "ACTIVE"
                logger.info("DECISION: CODE WINS (PERFECT 100%% match + count match) -> Promoting to ACTIVE")
            else:
                new_statement_status = "ACTIVE" # Also promoting 90%+ with matching count
                logger.info("DECISION: CODE WINS (accuracy=%.2f%% ≥ 90%% + count match) -> Promoting to ACTIVE", comparison_score)

        else:
            final_parser_type    = "LLM"
            new_statement_status = "EXPERIMENTAL"
            if not code_is_proper:
                reason = "code propriety check failed"
            elif not code_is_strict:
                reason = "code strict quality gate failed"
            else:
                reason = f"code accuracy {comparison_score:.2f}% < 90%"
            
            logger.info("DECISION: LLM WINS (%s)", reason)
            logger.info("Format status → EXPERIMENTAL")

            # ── AUTO-REPAIR FOR UNDER_REVIEW FORMATS ──────────────────────────
            # If the code failed but the LLM succeeded, use the LLM data
            # to internally fix the code for the next user.
            if has_llm and statement_status == "UNDER_REVIEW":
                try:
                    logger.info("AUTO-REPAIR: Triggering background fix for experimental format...")
                    repair_feedback = f"""
Auto-repair triggered. The stored parser script failed to match the LLM Ground Truth.
- Ground Truth Count: {len(llm_txns)}
- Code Parser Count: {len(code_txns)}

Please use these ground truth samples to fix the regex/logic:
{json.dumps(llm_txns[:5], indent=2)}
"""
                    fixed_logic = refine_extraction_logic_llm(
                        current_logic = extraction_code,
                        user_feedback = repair_feedback,
                        text_sample   = sample_text
                    )
                    if fixed_logic:
                        logger.info("AUTO-REPAIR: Script improved and updated in DB.")
                        update_extraction_logic(statement_id, fixed_logic)
                except Exception as ef:
                    logger.warning("AUTO-REPAIR: Background fix failed: %s", ef)

        # Persist DB state
        update_statement_status(statement_id, new_statement_status)
        update_success_rate(statement_id, comparison_score)
        update_processing_complete(document_id, final_parser_type)

        insert_staging_transactions(
            document_id=document_id,
            user_id=user_id,
            code_txns=code_txns,
            llm_txns=llm_txns,
            code_confidence=code_confidence,
            llm_confidence=llm_confidence,
        )

        update_document_status(document_id, "AWAITING_REVIEW")
        insert_audit(document_id, "COMPLETED")

        logger.info("")
        logger.info("PIPELINE COMPLETE for document_id=%s", document_id)
        logger.info("Winner       : %s", final_parser_type)
        logger.info("CODE txns    : %d", len(code_txns))
        logger.info("LLM txns     : %d", len(llm_txns))
        logger.info("Accuracy     : %.2f%%", comparison_score)
        logger.info("New status   : %s", new_statement_status)
        logger.info("═" * 70)

    except Exception as e:
        logger.error("")
        logger.error("PIPELINE FAILED for document_id=%s", document_id)
        logger.error("Error: %s", e, exc_info=True)
        logger.error("═" * 70)
        try:
            update_document_status(document_id, "FAILED")
            insert_audit(document_id, "FAILED", str(e))
        except Exception:
            logger.error("Failed to update failure status for document %s",
                         document_id, exc_info=True)
        raise