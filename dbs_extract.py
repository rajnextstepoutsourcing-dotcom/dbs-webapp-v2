import re
from typing import Dict, Any, Tuple

import pdfplumber
from dateutil import parser as dateparser

CERT_RE = re.compile(r"\b(\d{10,14})\b")


def _clean_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _safe_digits(s: str) -> str:
    return re.sub(r"\D", "", s or "")


def _extract_after_label(text: str, label: str) -> str:
    pattern = re.compile(rf"{re.escape(label)}\s*:?\s*(.+)", re.IGNORECASE)
    for line in (text or "").splitlines():
        m = pattern.search(line)
        if m:
            return _clean_space(m.group(1))
    return ""


def _parse_dmy(date_str: str) -> Tuple[str, str, str]:
    if not date_str:
        return "", "", ""
    try:
        dt = dateparser.parse(date_str, dayfirst=True, fuzzy=True)
        if not dt:
            return "", "", ""
        return f"{dt.day:02d}", f"{dt.month:02d}", f"{dt.year:04d}"
    except Exception:
        return "", "", ""


def extract_text_from_pdf(pdf_bytes: bytes, max_pages: int = 2) -> str:
    try:
        import io
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            texts = []
            for page in pdf.pages[:max_pages]:
                t = page.extract_text() or ""
                if t.strip():
                    texts.append(t)
            return "\n".join(texts).strip()
    except Exception:
        return ""


def extract_fields_from_text(text: str) -> Dict[str, Any]:
    # v2: Only Certificate Number, Surname, DOB (no Issue Date)
    text = text or ""
    out: Dict[str, Any] = {"certificate_number": None, "surname": None, "dob": None}

    # Certificate Number
    cert_line = _extract_after_label(text, "Certificate Number")
    cert = _safe_digits(cert_line)
    if not cert and "certificate number" in text.lower():
        idx = text.lower().find("certificate number")
        window = text[idx: idx + 220]
        m = CERT_RE.search(window)
        if m:
            cert = m.group(1)
    if cert and 10 <= len(cert) <= 14:
        out["certificate_number"] = cert

    # Surname
    surname = _extract_after_label(text, "Surname")
    surname = re.sub(r"[^A-Za-z\-\s]", "", surname).strip().upper()
    if surname:
        out["surname"] = surname

    # DOB
    dob_line = _extract_after_label(text, "Date of Birth") or _extract_after_label(text, "DOB")
    dd, mm, yyyy = _parse_dmy(dob_line)
    if dd and mm and yyyy:
        out["dob"] = {"dd": dd, "mm": mm, "yyyy": yyyy}

    return out
