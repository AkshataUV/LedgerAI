from fastapi import APIRouter, UploadFile, File, Form, HTTPException, status, Depends, BackgroundTasks
from pydantic import BaseModel
from fastapi.responses import JSONResponse
import os
import shutil
import tempfile
import pdfplumber
import json
import logging
import threading
import uuid
import hashlib
from typing import Optional
from db.connection import get_client, make_client, set_thread_client, clear_thread_client
from services.account_detector import get_user_accounts, link_document_to_account, create_user_account
from auth.utils import get_current_user

logger = logging.getLogger("ledgerai.document_routes")

router = APIRouter()

SUPABASE_STORAGE_BUCKET = "financial_document_uploads"  # Change this to your actual bucket name

# ── Transaction row validator ─────────────────────────────────────────────────
import re as _re
_DATE_RE = _re.compile(
    r"^\s*("
    r"\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}"
    r"|\d{4}[\/\-]\d{1,2}[\/\-]\d{1,2}"
    r"|\d{1,2}\s+[A-Za-z]{3}\s+\d{2,4}"
    r")\s*$"
)

def _is_valid_transaction(txn: dict) -> bool:
    # Support both "date" (current parser format) and "txn_date" (old staging rows)
    date_val = txn.get("date") or txn.get("txn_date")
    if not date_val:
        return False
    return bool(_DATE_RE.match(str(date_val).strip()))


def _is_password_exception(exc: Exception) -> bool:
    check_str = f"{str(exc)} {repr(exc)} {type(exc).__name__}".lower()
    if hasattr(exc, 'args') and exc.args:
        for arg in exc.args:
            check_str += f" {str(arg)} {type(arg).__name__}".lower()
    if hasattr(exc, '__cause__') and exc.__cause__:
        check_str += f" {str(exc.__cause__)} {type(exc.__cause__).__name__}".lower()
    keywords = ["password", "encrypt", "authenticate", "pdfminer", "pdfdocument",
                "pdfminerexception", "pdfpassword"]
    return any(kw in check_str for kw in keywords)


def _check_encryption_pypdf(tmp_path: str, password: str = None) -> str:
    try:
        from pypdf import PdfReader
    except ImportError:
        try:
            from PyPDF2 import PdfReader
        except ImportError:
            return None
    try:
        reader = PdfReader(tmp_path)
        if reader.is_encrypted:
            # Try decrypting with provided password OR empty string
            # Many bank PDFs are "encrypted" with no password (just for permissions)
            password_to_try = password or ""
            try:
                # decrypt returns 0 if failure, 1 or 2 if success
                if reader.decrypt(password_to_try) == 0:
                    return "PASSWORD_TEXT_PDF"
                
                # Even if pypdf says OK, verify with pdfplumber which is more sensitive
                try:
                    with pdfplumber.open(tmp_path, password=password_to_try) as pdf:
                        if pdf.pages:
                             # Just try to see if we can access the first page
                             _ = pdf.pages[0].chars
                except Exception:
                    return "PASSWORD_TEXT_PDF"
                    
                return None # Successfully opened/decrypted
            except Exception:
                return "PASSWORD_TEXT_PDF"
    except Exception:
        pass
    return None


def _detect_pdf_type_pdfplumber(tmp_path: str, password: str = None) -> str:
    with pdfplumber.open(tmp_path, password=password or "") as pdf:
        has_text = False
        has_images = False
        for page in pdf.pages:
            try:
                text = page.extract_text()
                if text and text.strip():
                    has_text = True
            except Exception as e:
                err_msg = str(e).lower()
                if any(kw in err_msg for kw in ["password", "encrypt", "pdfminer", "pdfdocument"]):
                    raise
            try:
                if page.images or page.figures:
                    has_images = True
            except Exception:
                pass
        if not has_text and has_images:
            return "IMAGE_CONVERTED_PDF"
        elif not has_text and not has_images:
            return "SCANNED_PDF"
        elif has_text and has_images:
            return "HYBRID_PDF"
        else:
            return "TEXT_PDF"


def _detect_pdf_type_pypdf_content(tmp_path: str, password: str = None) -> str:
    try:
        from pypdf import PdfReader
    except ImportError:
        try:
            from PyPDF2 import PdfReader
        except ImportError:
            raise ImportError("No fallback PDF reader available")
    reader = PdfReader(tmp_path)
    if reader.is_encrypted:
        password_to_try = password or ""
        try:
            if reader.decrypt(password_to_try) == 0:
                return "PASSWORD_TEXT_PDF"
        except Exception:
            return "PASSWORD_TEXT_PDF"
    has_text = False
    for page in reader.pages:
        text = page.extract_text()
        if text and text.strip():
            has_text = True
            break
    return "TEXT_PDF" if has_text else "SCANNED_PDF"


def _is_valid_pdf_binary(tmp_path: str) -> bool:
    try:
        with open(tmp_path, "rb") as f:
            header = f.read(1024)
            return b"%PDF" in header
    except Exception:
        return False


@router.post("/verify-type")
async def verify_pdf_type(file: UploadFile = File(...), password: Optional[str] = Form(None)):
    if not file.filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Only PDF files are allowed.")

    logger.info("")
    logger.info("─" * 50)
    logger.info("PDF TYPE DETECTION: %s", file.filename)
    logger.info("─" * 50)

    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        shutil.copyfileobj(file.file, tmp)
        tmp_path = tmp.name

    file_size = os.path.getsize(tmp_path)
    logger.info("File size   : %s bytes", f"{file_size:,}")

    pdf_type = "TEXT_PDF"
    try:
        logger.info("Step 0      : Checking encryption (pypdf)...")
        encryption_result = _check_encryption_pypdf(tmp_path, password)
        if encryption_result == "PASSWORD_TEXT_PDF":
            logger.info("Detected    : Password-protected PDF")
            logger.info("Final type  : PASSWORD_TEXT_PDF")
            return {"filename": file.filename, "pdf_type": "PASSWORD_TEXT_PDF"}
        elif encryption_result is None:
            logger.info("Encryption  : Not encrypted (or decrypted OK)")

        try:
            logger.info("Step 1      : Content analysis (pdfplumber)...")
            pdf_type = _detect_pdf_type_pdfplumber(tmp_path, password)
            logger.info("Result      : %s", pdf_type)
        except Exception as e1:
            logger.warning("pdfplumber failed: %s (type: %s)", str(e1)[:120], type(e1).__name__)
            if _is_password_exception(e1):
                pdf_type = "PASSWORD_TEXT_PDF"
            else:
                try:
                    pdf_type = _detect_pdf_type_pypdf_content(tmp_path, password)
                    logger.info("Result      : %s ", pdf_type)
                except ImportError:
                    pdf_type = "TEXT_PDF" if _is_valid_pdf_binary(tmp_path) else "CORRUPTED_PDF"
                except Exception as e2:
                    if _is_password_exception(e2):
                        pdf_type = "PASSWORD_TEXT_PDF"
                    else:
                        pdf_type = "TEXT_PDF" if _is_valid_pdf_binary(tmp_path) else "CORRUPTED_PDF"

        logger.info("   └─ Final type  : %s", pdf_type)
        logger.info("─" * 50)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

    return {"filename": file.filename, "pdf_type": pdf_type}


@router.post("/upload")
async def upload_and_process(
    file: UploadFile = File(...),
    password: Optional[str] = Form(None),
    user=Depends(get_current_user)
):
    """
    Full pipeline: Upload PDF → Insert into DB → Trigger processing engine.
    Returns the document_id immediately; processing runs in background.
    """
    import sys
    backend_root = os.path.dirname(os.path.dirname(__file__))
    if backend_root not in sys.path:
        sys.path.insert(0, backend_root)

    user_id = user["user_id"]

    if not file.filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Only PDF files are allowed.")

    logger.info("")
    logger.info("═" * 50)
    logger.info("DOCUMENT UPLOAD: %s", file.filename)
    logger.info("═" * 50)
    logger.info("user_id     : %s", user_id)
    logger.info("password    : %s", "YES" if password else "NO")

    unique_token = uuid.uuid4().hex
    file_hash = hashlib.sha256(f"{user_id}_{file.filename}_{unique_token}".encode()).hexdigest()[:24]
    safe_filename = f"{file_hash}.pdf"

    # Read file bytes into memory — no permanent local disk write
    file_bytes = await file.read()
    file_size = len(file_bytes)
    logger.info("File size   : %s bytes", f"{file_size:,}")

    # ── Upload to Supabase Storage ────────────────────────────
    # Storage path format: "<user_id>/<hash>.pdf"
    # This survives Render restarts/deploys (no ephemeral local disk dependency).
    storage_path = f"{user_id}/{safe_filename}"

    sb = get_client()
    try:
        sb.storage.from_(SUPABASE_STORAGE_BUCKET).upload(
            path=storage_path,
            file=file_bytes,
            file_options={"content-type": "application/pdf", "upsert": "false"},
        )
        logger.info("Uploaded to : supabase://%s/%s", SUPABASE_STORAGE_BUCKET, storage_path)
    except Exception as upload_err:
        logger.error("Supabase Storage upload failed: %s", upload_err)
        raise HTTPException(
            status_code=500,
            detail=f"File upload to storage failed: {upload_err}",
        )

    # ── Write a local temp file for the processing thread ────
    # The processing engine opens a local path to extract PDF text.
    # We create a temp file here and delete it inside run_processing()
    # once processing finishes (success or failure).
    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    tmp_file.write(file_bytes)
    tmp_file.close()
    tmp_file_path = tmp_file.name
    logger.info("Temp path   : %s (processing only, deleted after)", tmp_file_path)

    # ── Duplicate Check ───────────────────────────────────────
    # Check if a document with this name already exists for this user
    existing_doc = (
        sb.table("documents")
        .select("document_id")
        .eq("user_id", user_id)
        .eq("file_name", file.filename)
        .eq("is_active", True)
        .execute()
    )
    if existing_doc and existing_doc.data and len(existing_doc.data) > 0:
        logger.warning("DUPLICATE UPLOAD: File '%s' already exists (doc %s)", file.filename, existing_doc.data[0]["document_id"])

    is_pw = bool(password)

    doc_result = sb.table("documents").insert({
        "user_id": user_id,
        "file_name": file.filename,            # original name as uploaded by user e.g. "HDFC_March.pdf"
        "file_path": storage_path,             # hashed storage path: {user_id}/{hash}.pdf
        "is_password_protected": is_pw,
        "status": "UPLOADED",
    }).execute()
    document_id = doc_result.data[0]["document_id"]

    if is_pw:
        sb.table("document_password").insert({
            "document_id": document_id,
            "encrypted_password": password,
        }).execute()

    logger.info("═" * 50)
    logger.info("document_id : %s", document_id)
    logger.info("DB status   : UPLOADED")
    logger.info("Starting background processing thread...")
    logger.info("═" * 50)

    def run_processing():
        """
        Run processing engine with the temp local file, then clean up.
        We pass tmp_file_path directly to process_document so the DB
        file_path column always holds the permanent Supabase Storage path.
        Never patch the DB — that was the root cause of file_path going NULL.

        IMPORTANT: we call make_client() here to give this thread its own
        httpx connection pool, completely isolated from the FastAPI request
        handlers that use the get_client() singleton.  supabase-py uses HTTP/2
        which multiplexes all requests onto one socket — sharing it between a
        long-running pipeline and concurrent status-poll requests causes
        EAGAIN / ReadError regardless of how many documents are uploading.

        set_thread_client() registers the fresh client as the thread-local
        override so every repo call inside process_document() automatically
        picks it up via get_client() — no changes needed in any repo file.
        """
        thread_sb = make_client()
        set_thread_client(thread_sb)
        try:
            from services.processing_engine import process_document
            process_document(document_id, override_file_path=tmp_file_path)
        except Exception as e:
            logger.error("[ERROR] Processing failed for doc %s: %s", document_id, e)
        finally:
            clear_thread_client()
            # Always delete the temp file — whether processing succeeded or not
            if os.path.exists(tmp_file_path):
                try:
                    os.remove(tmp_file_path)
                    logger.info("Temp file removed: %s", tmp_file_path)
                except Exception as rm_err:
                    logger.warning("Could not remove temp file %s: %s", tmp_file_path, rm_err)

    thread = threading.Thread(target=run_processing, daemon=True)
    thread.start()

    logger.info("Upload complete, processing started in background")
    logger.info("═" * 50)

    return {"document_id": document_id, "status": "PROCESSING", "message": "Document uploaded. Processing started."}


@router.get("/status/{document_id}")
async def get_document_status(document_id: int, user=Depends(get_current_user)):
    user_id = user["user_id"]
    sb = get_client()
    result = (
        sb.table("documents")
        .select("document_id, status, transaction_parsed_type, file_name")
        .eq("document_id", document_id)
        .eq("user_id", user_id)
        .maybe_single()
        .execute()
    )
    if not result.data:
        raise HTTPException(status_code=404, detail="Document not found")
    return result.data


@router.get("/stats")
async def get_document_stats(user=Depends(get_current_user)):
    user_id = user["user_id"]
    sb = get_client()
    result = (
        sb.table("documents")
        .select("status")
        .eq("user_id", user_id)
        .execute()
    )
    rows = result.data or []
    total = len(rows)
    parsed = sum(1 for r in rows if r["status"] == "APPROVE")
    failed = sum(1 for r in rows if r["status"] == "FAILED")
    pending_review = sum(1 for r in rows if r["status"] == "AWAITING_REVIEW")
    return {"total": total, "parsed": parsed, "failed": failed, "pending_review": pending_review}


@router.get("/recent")
async def get_recent_documents(user=Depends(get_current_user)):
    user_id = user["user_id"]
    sb = get_client()
    result = (
        sb.table("documents")
        .select(
            "document_id, file_name, status, transaction_parsed_type, created_at, "
            "statement_categories(institution_name, logic_version)"
        )
        .eq("user_id", user_id)
        .order("created_at", desc=True)
        .limit(20)
        .execute()
    )
    # Flatten the nested statement_categories join
    rows = []
    for r in (result.data or []):
        cat = r.pop("statement_categories", None) or {}
        r["institution_name"] = cat.get("institution_name")
        r["logic_version"] = cat.get("logic_version", 1)
        rows.append(r)
    return rows


@router.get("/{document_id}/review")
async def get_document_review(document_id: int, user=Depends(get_current_user)):
    user_id = user["user_id"]
    sb = get_client()

    doc_result = (
        sb.table("documents")
        .select("*, statement_categories(institution_name, statement_identifier, logic_version)")
        .eq("document_id", document_id)
        .eq("user_id", user_id)
        .maybe_single()
        .execute()
    )
    if not doc_result.data:
        raise HTTPException(status_code=404, detail="Document not found")
    doc = doc_result.data

    # Get staging transactions
    staging_result = (
        sb.table("ai_transactions_staging")
        .select("staging_transaction_id, transaction_json, parser_type, overall_confidence")
        .eq("document_id", document_id)
        .execute()
    )
    staging_rows = staging_result.data or []

    code_txns = []
    llm_txns = []
    for row in staging_rows:
        txn_data = row["transaction_json"]
        if isinstance(txn_data, str):
            txn_data = json.loads(txn_data)
        if row["parser_type"] == "CODE":
            code_txns = txn_data
        else:
            llm_txns = txn_data

    cat = doc.pop("statement_categories", None) or {}
    bank_name  = cat.get("institution_name") or "Pending Identification"
    ident_json = cat.get("statement_identifier")

    # ── Fetch all accounts for this user for the dropdown ───────────────────
    user_id_for_accounts = user["user_id"]
    user_accounts = get_user_accounts(user_id_for_accounts)

    res = {
        "bank_name":              bank_name,
        "identifier_json":        ident_json,
        "code_transactions":      code_txns,
        "llm_transactions":       llm_txns,
        "status":                 doc["status"],
        "transaction_parsed_type": doc.get("transaction_parsed_type"),
        "selected_account_id":    doc.get("account_id"),   # already linked account if any
        "user_accounts":          user_accounts,           # list for dropdown
    }

    # ── START DEDUPLICATION LOGIC ─────────────────────────────────────────────
    # This part was restored after being lost in a Git merge
    account_id = res["selected_account_id"]
    if account_id:
        # 1. Uncategorized (Pending)
        uncat_exists = (
            sb.table("uncategorized_transactions")
            .select("document_id, txn_date, details, debit, credit")
            .eq("account_id", account_id)
            .eq("user_id", user_id)
            .execute()
        )
        
        # 2. Categorized (Ledger)
        ledger_exists = (
            sb.table("transactions")
            .select("document_id, transaction_date, details, amount")
            .eq("base_account_id", account_id)
            .eq("user_id", user_id)
            .execute()
        )
        
        existing_fingerprints = []
        import re as _re
        def get_normalized_alphanumeric(text):
            return _re.sub(r'[^A-Z0-9]', '', (text or "").upper())

        def has_shared_core(s1, s2, min_len=12):
            if not s1 or not s2: return False
            if s1 in s2 or s2 in s1: return True
            if len(s1) < min_len or len(s2) < min_len: return s1 == s2
            for i in range(len(s1) - min_len + 1):
                if s1[i:i+min_len] in s2: return True
            return False

        for r in (uncat_exists.data or []):
            if r.get("document_id") == document_id: continue
            amt = float(r.get("debit") or 0) or float(r.get("credit") or 0)
            existing_fingerprints.append({
                "date": str(r["txn_date"]),
                "amount": f"{amt:.2f}",
                "fuzzy": get_normalized_alphanumeric(r["details"])
            })
            
        for r in (ledger_exists.data or []):
            if r.get("document_id") == document_id: continue
            amt = float(r.get("amount") or 0)
            existing_fingerprints.append({
                "date": str(r["transaction_date"]),
                "amount": f"{amt:.2f}",
                "fuzzy": get_normalized_alphanumeric(r["details"])
            })

        duplicates_found = 0
        def mark_duplicates(txn_list):
            nonlocal duplicates_found
            for t in txn_list:
                t_amt = f"{float(t.get('debit') or 0) or float(t.get('credit') or 0):.2f}"
                t_date = str(t["date"])
                t_fuzzy = get_normalized_alphanumeric(t["details"])
                is_duplicate = False
                for ex in existing_fingerprints:
                    if ex["date"] == t_date and ex["amount"] == t_amt:
                        if has_shared_core(t_fuzzy, ex["fuzzy"]):
                            is_duplicate = True
                            break
                if is_duplicate:
                    t["is_duplicate"] = True
                    duplicates_found += 1
                else:
                    t["is_duplicate"] = False

        mark_duplicates(res["code_transactions"])
        mark_duplicates(res["llm_transactions"])
        
        if duplicates_found > 0:
            logger.info("Deduplication: Detected %s potential duplicates for doc %s (account %s)", 
                        duplicates_found, document_id, account_id)
            # Debug: Verify the first few results have the flag
            for list_name in ["code_transactions", "llm_transactions"]:
                for i, tx in enumerate(res[list_name][:3]):
                    if tx.get("is_duplicate"):
                        logger.info("  [DEBUG] %s[%s] is flagged as DUPLICATE", list_name, i)
            
            # Add the summary count for the frontend banner
            res["duplicates_count"] = duplicates_found

    return res


class ApprovalRequest(BaseModel):
    transactions: Optional[list] = None
    parser_type: Optional[str] = None


class RetryRequest(BaseModel):
    method: str  # "CODE", "VISION", "MANUAL"
    note: Optional[str] = None


class CreateAccountRequest(BaseModel):
    institution_name: str
    account_name: Optional[str] = None
    type: str  # "BANK" or "CREDIT_CARD"
    last4: str
    ifsc_code: Optional[str] = None
    card_network: Optional[str] = None


@router.post("/accounts")
async def add_new_account(
    body: CreateAccountRequest,
    user=Depends(get_current_user)
):
    user_id = user["user_id"]
    try:
        new_account = create_user_account(user_id, body.dict())
        return new_account
    except Exception as e:
        logger.error(f"Failed to add account: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{document_id}/retry")
async def retry_extraction(
    document_id: int,
    body: RetryRequest,
    user=Depends(get_current_user)
):
    user_id = user["user_id"]
    sb = get_client()

    doc_result = (
        sb.table("documents")
        .select("document_id, file_path, status")
        .eq("document_id", document_id)
        .eq("user_id", user_id)
        .maybe_single()
        .execute()
    )
    if not doc_result.data:
        raise HTTPException(status_code=404, detail="Document not found")

    doc = doc_result.data
    storage_path = doc["file_path"]

    # 1. Update status to UPLOADED
    sb.table("documents").update({"status": "UPLOADED"}).eq("document_id", document_id).execute()

    # 2. Re-trigger processing engine
    # We need to download the file from Storage to a temp file for the engine to read
    # or pass the storage path and let the engine handle it.
    # Current engine expects a local file path.

    def run_retry():
        thread_sb = make_client()
        set_thread_client(thread_sb)
        tmp_file_path = None
        try:
            # Download from Supabase Storage
            res = thread_sb.storage.from_(SUPABASE_STORAGE_BUCKET).download(storage_path)
            
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
                tmp.write(res)
                tmp_file_path = tmp.name

            from services.processing_engine import process_document
            process_document(
                document_id, 
                override_file_path=tmp_file_path,
                retry_mode=body.method,
                retry_note=body.note
            )
        except Exception as e:
            logger.error("[ERROR] Retry processing failed for doc %s: %s", document_id, e)
            thread_sb.table("documents").update({"status": "FAILED"}).eq("document_id", document_id).execute()
        finally:
            clear_thread_client()
            if tmp_file_path and os.path.exists(tmp_file_path):
                os.remove(tmp_file_path)

    thread = threading.Thread(target=run_retry, daemon=True)
    thread.start()

    return {"message": "Retry started", "document_id": document_id}


@router.post("/{document_id}/approve")
async def approve_document(
    document_id: int,
    background_tasks: BackgroundTasks,
    body: Optional[ApprovalRequest] = None,
    user=Depends(get_current_user)
):
    user_id = user["user_id"]
    sb = get_client()

    doc_result = (
        sb.table("documents")
        .select("document_id, status, statement_id, transaction_parsed_type, account_id")
        .eq("document_id", document_id)
        .eq("user_id", user_id)
        .maybe_single()
        .execute()
    )
    if not doc_result.data:
        raise HTTPException(status_code=404, detail="Document not found")
    doc = doc_result.data

    if doc["status"] == "APPROVE":
        return {"message": "Already approved", "inserted": 0}

    transactions_to_approve = []
    parser_used = doc.get("transaction_parsed_type") or "LLM"
    staging_id = None

    # Case 1: Selective transactions provided by frontend
    if body and body.transactions is not None:
        transactions_to_approve = body.transactions
        parser_used = body.parser_type or parser_used
        logger.info("Approve doc=%s: User provided %d selective/edited transactions (parser=%s)", 
                    document_id, len(transactions_to_approve), parser_used)
        
        # We still want to link these back to a staging_id if possible for record-keeping
        staging_result = (
            sb.table("ai_transactions_staging")
            .select("staging_transaction_id")
            .eq("document_id", document_id)
            .eq("parser_type", parser_used)
            .maybe_single()
            .execute()
        )
        if staging_result.data:
            staging_id = staging_result.data["staging_transaction_id"]

    # Case 2: Standard "Approve All" logic
    else:
        staging_result = (
            sb.table("ai_transactions_staging")
            .select("staging_transaction_id, transaction_json, parser_type")
            .eq("document_id", document_id)
            .execute()
        )
        staging_rows = staging_result.data or []
        if not staging_rows:
            raise HTTPException(status_code=400, detail="No staging transactions to approve")

        chosen_row = next(
            (r for r in staging_rows if r["parser_type"] == parser_used),
            staging_rows[0],
        )
        parser_used = chosen_row["parser_type"]
        staging_id = chosen_row["staging_transaction_id"]
        
        txn_data = chosen_row["transaction_json"]
        if isinstance(txn_data, str):
            txn_data = json.loads(txn_data)
        
        if isinstance(txn_data, dict):
            transactions_to_approve = [txn_data]
        elif isinstance(txn_data, list):
            transactions_to_approve = txn_data

    # Common validation and insertion logic
    clean_txns = [t for t in transactions_to_approve if _is_valid_transaction(t)]
    skipped = len(transactions_to_approve) - len(clean_txns)
    
    if skipped and not (body and body.transactions):
        # Only log junk skipped for automatic path; for manual path, user might have just selected few
        logger.warning("Approve doc=%s — skipped %d junk row(s)", document_id, skipped)
    
    account_id = doc.get("account_id")

    rows = [
        {
            "user_id": user_id,
            "account_id": account_id,
            "document_id": document_id,
            "staging_transaction_id": staging_id,
            "txn_date": txn.get("date") or txn.get("txn_date"),
            "debit": txn.get("debit"),
            "credit": txn.get("credit"),
            "balance": txn.get("balance"),
            "details": (txn.get("details") or "")[:500],
        }
        for txn in clean_txns
    ]

    if not rows:
        raise HTTPException(
            status_code=400,
            detail="No valid transactions selected for approval."
        )

    try:
        sb.table("uncategorized_transactions").insert(rows).execute()
        sb.table("documents").update({"status": "APPROVE"}).eq("document_id", document_id).execute()
    except Exception as insert_err:
        logger.error("Approve doc=%s — INSERT FAILED: %s", document_id, insert_err)
        raise HTTPException(status_code=500, detail=f"Failed to save transactions: {insert_err}")

    inserted = len(rows)
    logger.info("✅ Document %s approved — %d txns saved (parser=%s)",
                document_id, inserted, parser_used)

    # ── Fire pre-pipeline background grouping job ────────────────────────────
    # Uses BackgroundTasks so the HTTP response is returned immediately.
    # The grouping job creates its own isolated Supabase client (make_client)
    # to avoid sharing the request handler's connection pool.
    def _run_grouping_task(doc_id: int, uid: str) -> None:
        from db.connection import make_client, set_thread_client, clear_thread_client
        from services.merchant_grouping import run_merchant_grouping
        thread_sb = make_client()
        set_thread_client(thread_sb)
        try:
            run_merchant_grouping(doc_id, uid)
        except Exception as grp_err:
            logger.error("[GROUPING] Failed for doc_id=%s: %s", doc_id, grp_err)
        finally:
            clear_thread_client()

    background_tasks.add_task(_run_grouping_task, document_id, user_id)
    logger.info("Grouping background task queued for document_id=%s", document_id)

    return {"message": "Document approved", "inserted": inserted}


@router.get("/{document_id}/user-accounts")
async def get_user_accounts_for_document(
    document_id: int,
    user=Depends(get_current_user),
):
    """
    Return all accounts belonging to this user for the account dropdown
    on the Review screen.
    """
    user_id = user["user_id"]
    sb = get_client()

    # Verify document belongs to this user
    doc = (
        sb.table("documents")
        .select("document_id, account_id")
        .eq("document_id", document_id)
        .eq("user_id", user_id)
        .maybe_single()
        .execute()
    ).data
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")

    accounts = get_user_accounts(user_id)
    return {
        "user_accounts":       accounts,
        "selected_account_id": doc.get("account_id"),
    }


class SelectAccountRequest(BaseModel):
    account_id: int


@router.post("/{document_id}/select-account")
async def select_account(
    document_id: int,
    body: SelectAccountRequest,
    user=Depends(get_current_user),
):
    """
    Called when user picks an account from the dropdown on the Review screen.
    Links the document to the chosen account_id.
    """
    user_id = user["user_id"]
    sb = get_client()

    # Verify document belongs to this user
    doc = (
        sb.table("documents")
        .select("document_id, account_id")
        .eq("document_id", document_id)
        .eq("user_id", user_id)
        .maybe_single()
        .execute()
    ).data
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")

    # Verify the selected account belongs to this user
    acct = (
        sb.table("accounts")
        .select("account_id")
        .eq("account_id", body.account_id)
        .eq("user_id", user_id)
        .eq("is_active", True)
        .maybe_single()
        .execute()
    ).data
    if not acct:
        raise HTTPException(status_code=404, detail="Account not found")

    link_document_to_account(document_id, body.account_id)

    logger.info(
        "select_account: doc=%s linked to account_id=%s by user=%s",
        document_id, body.account_id, user_id,
    )
    return {"message": "Account linked", "account_id": body.account_id}


@router.delete("/{document_id}")
async def delete_document(document_id: int, user=Depends(get_current_user)):
    user_id = user["user_id"]
    sb = get_client()

    try:
        # 1. Fetch document metadata to handle storage cleanup
        doc_result = (
            sb.table("documents")
            .select("document_id, file_path, status")
            .eq("document_id", document_id)
            .eq("user_id", user_id)
            .maybe_single()
            .execute()
        )
        
        if not doc_result or not hasattr(doc_result, 'data') or doc_result.data is None:
            logger.warning("Delete failed: Document %s not found for user %s", document_id, user_id)
            throw_err = True
        else:
            throw_err = False

        if throw_err:
            raise HTTPException(status_code=404, detail="Document not found or already deleted")

        doc = doc_result.data
        file_path = doc.get("file_path")

        # 2. Cleanup Supabase Storage / Local files
        if file_path:
            if not str(file_path).startswith("/"):
                try:
                    # Supabase Storage path: "user_id/hash.pdf"
                    sb.storage.from_(SUPABASE_STORAGE_BUCKET).remove([file_path])
                    logger.info("Deleted from Supabase Storage: %s", file_path)
                except Exception as e:
                    logger.warning("Could not delete storage file %s: %s", file_path, e)
            elif os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    logger.info("Deleted local file: %s", file_path)
                except Exception as e:
                    logger.warning("Could not delete local file %s: %s", file_path, e)

        # 3. Delete child records and document record
        # Note: We don't check .data for these deletes to avoid NoneType errors 
        # if the library returns None or empty response objects for DELETE operations.
        sb.table("ai_transactions_staging").delete().eq("document_id", document_id).execute()
        sb.table("uncategorized_transactions").delete().eq("document_id", document_id).execute()
        sb.table("document_password").delete().eq("document_id", document_id).execute()
        
        # Finally delete the document record
        sb.table("documents").delete().eq("document_id", document_id).eq("user_id", user_id).execute()
        
        logger.info("✅ Document %s successfully deleted by user %s", document_id, user_id)
        return {"message": "Document deleted successfully"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error("❌ Database deletion failed for doc %s: %s", document_id, e, exc_info=True)
        # Check for the specific NoneType error and return a cleaner message
        err_msg = str(e)
        if "'NoneType' object has no attribute 'data'" in err_msg:
            err_msg = "Database response was empty. The document might already be deleted."
        raise HTTPException(status_code=500, detail=f"Database deletion failed: {err_msg}")


@router.get("/{document_id}/download-json")
async def download_transactions_json(document_id: int, user=Depends(get_current_user)):
    user_id = user["user_id"]
    sb = get_client()

    doc_result = (
        sb.table("documents")
        .select("document_id, file_name, transaction_parsed_type")
        .eq("document_id", document_id)
        .eq("user_id", user_id)
        .maybe_single()
        .execute()
    )
    if not doc_result.data:
        raise HTTPException(status_code=404, detail="Document not found")
    doc = doc_result.data

    preferred_parser = doc.get("transaction_parsed_type") or "CODE"

    staging_result = (
        sb.table("ai_transactions_staging")
        .select("transaction_json, parser_type")
        .eq("document_id", document_id)
        .execute()
    )
    staging_rows = staging_result.data or []
    if not staging_rows:
        raise HTTPException(status_code=404, detail="No extracted transactions found for this document")

    chosen_json = None
    fallback_json = None
    for row in staging_rows:
        txn_data = row["transaction_json"]
        if isinstance(txn_data, str):
            txn_data = json.loads(txn_data)
        if row["parser_type"] == preferred_parser:
            chosen_json = txn_data
        else:
            fallback_json = txn_data

    result_json = chosen_json if chosen_json is not None else fallback_json
    if result_json is None:
        raise HTTPException(status_code=404, detail="No transaction JSON available")

    safe_name = doc["file_name"].replace(".pdf", "").replace(" ", "_")
    return JSONResponse(
        content={
            "document_id": document_id,
            "file_name": doc["file_name"],
            "parser_type": preferred_parser,
            "transaction_count": len(result_json),
            "transactions": result_json,
        },
        headers={
            "Content-Disposition": f'attachment; filename="{safe_name}_transactions.json"'
        }
    )