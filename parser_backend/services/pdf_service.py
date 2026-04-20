
import logging
import pdfplumber
from PyPDF2 import PdfReader
import re
from typing import List, Optional

logger = logging.getLogger("ledgerai.pdf_service")


# ──────────────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────────────

# Phrases that identify footer/disclaimer lines
FOOTER_PHRASES = [
    # Generic disclaimers
    'this is a computer generated',
    'does not require signature',
    'need not normally be signed',
    'contents of this statement will be',
    'no error is reported within',
    'treated that the entries',
    'please do not share your atm',
    'bank never asks for',
    'if you receive any alerts',
    # Addresses / contacts
    'registered office',
    'registered office address',
    'head office',
    # Bank-specific footer lines that run words together (HDFC PDF artefact)
    'hdfcbanklimited',
    'closingbalanceincludes',
    'contentsofthisstatement',
    'stateaccountbranchgstn',
    'hdfcbankgstinnumber',
    'stateaccountbranch',
    # GSTIN / legal
    'gstin',
    'cin:',
    # Toll-free / contact
    'customer care',
    'toll free',
    '1800 ',
    '022-',
    # URLs
    'www.',
    'http',
    # Statement markers
    'end of statement',
    '*** end of',
    'powered by',
    # Misc
    'beware of cyber',
    'nevershare',
    'never share your',
    'scan for',
    'disclaimer',
    'important information',
    'deposit insurance',
    'yes bank gstin',
]

# Regex: (cid:NNN) garbage from bad font encoding
CID_PATTERN = re.compile(r'\(cid:\d+\)')

# Regex: hyphen at end of word indicating PDF line-wrap continuation
HYPHEN_WRAP = re.compile(r'-$')

# How close (in PDF points ≈ 1/72 inch) two word Y-centres must be
# to be considered on the same visual row
ROW_Y_TOLERANCE = 4   # ~1.4 mm — works for all banks tested

# Minimum gap (points) between words before we insert a space character
MIN_WORD_GAP = 3.0


# ──────────────────────────────────────────────────────────────────────────────
# Low-level word → row helpers
# ──────────────────────────────────────────────────────────────────────────────

def _y_centre(w: dict) -> float:
    return (w['top'] + w['bottom']) / 2.0


def _group_words_into_rows(words: list, y_tol: float = ROW_Y_TOLERANCE) -> List[List[dict]]:
    """
    Cluster word dicts by Y-centre proximity → visual rows.
    Each returned row is sorted left→right by x0.
    """
    if not words:
        return []
    sorted_words = sorted(words, key=lambda w: (_y_centre(w), w['x0']))
    rows: List[List[dict]] = []
    cur: List[dict] = [sorted_words[0]]
    cur_y = _y_centre(sorted_words[0])

    for w in sorted_words[1:]:
        y = _y_centre(w)
        if abs(y - cur_y) <= y_tol:
            cur.append(w)
        else:
            rows.append(sorted(cur, key=lambda x: x['x0']))
            cur = [w]
            cur_y = y

    if cur:
        rows.append(sorted(cur, key=lambda x: x['x0']))
    return rows


def _row_to_text(row: List[dict], min_gap: float = MIN_WORD_GAP) -> str:
    """
    Reconstruct a line of text from a row of word dicts.
    Gaps between words are converted to proportional whitespace so column
    positions are approximately preserved.
    """
    if not row:
        return ''
    parts = [row[0]['text']]
    for i in range(1, len(row)):
        gap = row[i]['x0'] - row[i - 1]['x1']
        if gap >= min_gap * 8:
            parts.append('        ')   # very wide column gap
        elif gap >= min_gap * 4:
            parts.append('    ')
        elif gap >= min_gap * 2:
            parts.append('  ')
        elif gap >= min_gap * 0.5:
            parts.append(' ')
        parts.append(row[i]['text'])
    return ''.join(parts)


def _join_hyphen_wraps(lines: List[str]) -> List[str]:
    result: List[str] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        # Strip trailing whitespace for the hyphen test
        stripped = line.rstrip()
        if (stripped.endswith('-')
                and i + 1 < len(lines)
                and lines[i + 1].strip()
                and lines[i + 1].strip()[0].islower()):
            # Merge: remove the hyphen, concatenate with next line's content
            next_stripped = lines[i + 1].lstrip()
            result.append(stripped[:-1] + next_stripped)
            i += 2
        else:
            result.append(line)
            i += 1
    return result


def _clean_cid(text: str) -> str:
    """FIX 2: Remove (cid:NNN) font-encoding garbage."""
    return CID_PATTERN.sub('', text)


# ──────────────────────────────────────────────────────────────────────────────
# Footer removal
# ──────────────────────────────────────────────────────────────────────────────

def _is_footer_line(line: str) -> bool:
    lower = line.lower().strip()
    if not lower:
        return False
    for phrase in FOOTER_PHRASES:
        if phrase in lower:
            return True
    # Very short purely numeric / punctuation lines
    if len(lower) < 5 and re.match(r'^[\d\s\-\.\*\/]+$', lower):
        return True
    return False


def _remove_footer_blocks(lines: List[str], min_block: int = 2) -> List[str]:
    
    is_footer = [_is_footer_line(l) for l in lines]
    result: List[str] = []
    i = 0
    while i < len(lines):
        if is_footer[i]:
            j = i
            while j < len(lines) and is_footer[j]:
                j += 1
            if (j - i) >= min_block:
                i = j
                continue
        result.append(lines[i])
        i += 1
    return result

# ──────────────────────────────────────────────────────────────────────────────
# Scoring helper — picks the best of three extraction candidates
# ──────────────────────────────────────────────────────────────────────────────

def _score(text: str) -> int:
    """
    Score a text candidate by how much it looks like a bank statement page.
    Higher is better.
    """
    if not text:
        return 0
    dates   = len(re.findall(r'\b\d{2}[/\-]\d{2}[/\-]\d{2,4}\b', text))
    amounts = len(re.findall(r'\b\d{1,3}(?:,\d{3})+(?:\.\d{2})?\b', text))
    lines   = len(text.strip().splitlines())
    # Penalise text that is mostly CID garbage
    cid_count = len(CID_PATTERN.findall(text))
    return dates * 12 + amounts * 3 + lines - cid_count * 5


# ──────────────────────────────────────────────────────────────────────────────
# Main extractor class
# ──────────────────────────────────────────────────────────────────────────────

class EnhancedFinancialPDFExtractor:

    def __init__(self, pdf_path: str):
        self.pdf_path   = pdf_path
        self.password: Optional[str] = None

    # ── Password handling ─────────────────────────────────────────────────────

    def _is_encrypted(self) -> bool:
        try:
            return PdfReader(self.pdf_path).is_encrypted
        except Exception:
            return False

    def _ask_password(self) -> str:
        # In service context password is supplied by caller — never prompt interactively
        return self.password or ''

    # ── Extraction strategies ─────────────────────────────────────────────────

    def _word_row_text(self, page) -> str:
        """Strategy A: reconstruct rows from word bounding boxes."""
        try:
            words = page.extract_words(
                x_tolerance=3, y_tolerance=3,
                keep_blank_chars=False, use_text_flow=False,
            )
        except Exception:
            return ''
        if not words:
            return ''
        rows  = _group_words_into_rows(words, ROW_Y_TOLERANCE)
        lines = [_row_to_text(row) for row in rows]
        return '\n'.join(lines)

    def _table_text(self, page) -> str:
        """Strategy B: pdfplumber embedded table extraction."""
        try:
            tables = page.extract_tables()
            if not tables:
                return ''
            # Use the largest table on the page
            tbl = max(tables, key=lambda t: sum(len(r) for r in t if r))
            non_empty = sum(1 for row in tbl for c in row if c and str(c).strip())
            if non_empty < 4:
                return ''
            return self._format_table(tbl)
        except Exception:
            return ''

    @staticmethod
    def _format_table(table: list) -> str:
        if not table:
            return ''
        col_count = max((len(r) for r in table), default=0)
        # Column widths
        widths = [0] * col_count
        for row in table:
            for ci in range(min(len(row), col_count)):
                cell = str(row[ci] or '').replace('\n', ' ').strip()
                widths[ci] = max(widths[ci], len(cell) + 2)
        # Render
        lines = []
        for row in table:
            parts = []
            for ci in range(col_count):
                cell = str(row[ci] or '').replace('\n', ' ').strip() if ci < len(row) else ''
                w = widths[ci]
                if re.match(r'^[\d,.\-+₹$()]+$', cell):
                    parts.append(cell.rjust(w))
                else:
                    parts.append(cell.ljust(w))
            lines.append(''.join(parts).rstrip())
        return '\n'.join(lines)

    def _layout_text(self, page) -> str:
        """Strategy C: pdfplumber layout-aware plain text."""
        try:
            return page.extract_text(layout=True) or ''
        except Exception:
            return ''

    # ── Per-page pipeline ─────────────────────────────────────────────────────

    def _process_page(self, page) -> str:
        # Collect three candidates
        a = _clean_cid(self._word_row_text(page))
        b = _clean_cid(self._table_text(page))
        c = _clean_cid(self._layout_text(page))

        # Pick best candidate
        best = max([a, b, c], key=_score)

        # Post-processing pipeline
        lines = best.split('\n')
        lines = _join_hyphen_wraps(lines)           # FIX 1
        lines = _remove_footer_blocks(lines, min_block=2)  # FIX 3

        # Strip trailing spaces, collapse excess blank lines (FIX 5)
        out: List[str] = []
        blanks = 0
        for line in lines:
            line = line.rstrip()
            if line.strip() == '':
                blanks += 1
                if blanks == 1:        # allow at most 1 consecutive blank
                    out.append('')
            else:
                blanks = 0
                out.append(line)

        return '\n'.join(out)

    # ── Full-document entry point ─────────────────────────────────────────────

    def extract_all_text(self) -> str:
        if self._is_encrypted():
            self.password = self._ask_password()

        pages_text: List[str] = []

        try:
            with pdfplumber.open(self.pdf_path, password=self.password) as pdf:
                total = len(pdf.pages)
                logger.info("Processing %d page(s)...", total)
                for num, page in enumerate(pdf.pages, start=1):
                    logger.debug("Page %d/%d ...", num, total)
                    text = self._process_page(page)
                    if text.strip():
                        sep = ('' if num == 1 else '\n') + '=' * 80 + f'\nPAGE {num}\n' + '=' * 80 + '\n'
                        pages_text.append(sep + text)

                logger.info("Done — %d page(s) processed.", total)
                return '\n'.join(pages_text)

        except Exception as exc:
            msg = str(exc).lower()
            if 'password' in msg or 'encrypt' in msg:
                logger.error("Incorrect password or unable to decrypt PDF: %s", self.pdf_path)
            else:
                logger.error("Error extracting PDF %s: %s", self.pdf_path, exc, exc_info=True)
            return ''
# ──────────────────────────────────────────────────────────────────────────────
# Service entry point
# ──────────────────────────────────────────────────────────────────────────────

def extract_pdf_text(pdf_path: str, password: str = None) -> str:
    """
    Called by processing_engine.py.
    Creates EnhancedFinancialPDFExtractor, injects the password so the
    interactive getpass prompt is never triggered, and returns the full
    extracted text with page separators exactly as the extractor produces them.
    """
    logger.info("extract_pdf_text: %s  password=%s", pdf_path, bool(password))
    extractor = EnhancedFinancialPDFExtractor(pdf_path)
    if password:
        extractor.password = password
    return extractor.extract_all_text()