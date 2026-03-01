import sys
import asyncio

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

import os
import io
import json
import re
import datetime
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List
import anyio

from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

import fitz  # PyMuPDF
import pdfplumber

# Gemini (google-genai) – same import style as your web_app.py
try:
    from google import genai
    from google.genai import types
except Exception:
    genai = None
    types = None

APP_DIR = os.path.dirname(os.path.abspath(__file__))

app = FastAPI(title="DBS Check")
app.mount("/static", StaticFiles(directory=os.path.join(APP_DIR, "static")), name="static")
templates = Jinja2Templates(directory=os.path.join(APP_DIR, "templates"))


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    return v


# --- Env (FAST -> STRONG fallback) ---
GEMINI_API_KEY = _env("GEMINI_API_KEY")
GEMINI_MODEL_FAST = _env("GEMINI_MODEL_FAST", "gemini-2.0-flash-001")
GEMINI_MODEL_STRONG = _env("GEMINI_MODEL_STRONG", "gemini-2.5-pro")


def get_gemini_client():
    if not GEMINI_API_KEY or genai is None or types is None:
        return None
    try:
        return genai.Client(api_key=GEMINI_API_KEY)
    except Exception:
        return None


GEMINI_CLIENT = get_gemini_client()


def normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


MONTHS = {
    "JANUARY": "01", "FEBRUARY": "02", "MARCH": "03", "APRIL": "04", "MAY": "05", "JUNE": "06",
    "JULY": "07", "AUGUST": "08", "SEPTEMBER": "09", "OCTOBER": "10", "NOVEMBER": "11", "DECEMBER": "12",
    "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04", "JUN": "06", "JUL": "07", "AUG": "08",
    "SEP": "09", "SEPT": "09", "OCT": "10", "NOV": "11", "DEC": "12",
}


def parse_uk_date_words(date_str: str) -> Optional[Tuple[str, str, str]]:
    """Parse '11 SEPTEMBER 2023' -> ('11','09','2023')"""
    if not date_str:
        return None
    s = normalize_ws(date_str).upper()
    m = re.search(r"\b(\d{1,2})\s+([A-Z]{3,9})\s+(\d{4})\b", s)
    if not m:
        return None
    dd = m.group(1).zfill(2)
    mon = MONTHS.get(m.group(2))
    yyyy = m.group(3)
    if not mon:
        return None
    return dd, mon, yyyy


def parse_ddmmyyyy(date_str: str) -> Optional[Tuple[str, str, str]]:
    if not date_str:
        return None
    s = normalize_ws(date_str)
    m = re.search(r"\b(\d{1,2})[\/\.-](\d{1,2})[\/\.-](\d{2,4})\b", s)
    if not m:
        return None
    dd = m.group(1).zfill(2)
    mm = m.group(2).zfill(2)
    yy = m.group(3)
    if len(yy) == 2:
        yy = ("19" if int(yy) > 30 else "20") + yy
    return dd, mm, yy


def validate_cert_number(s: str) -> Optional[str]:
    if not s:
        return None
    digits = re.sub(r"\D", "", s)
    if 10 <= len(digits) <= 14:
        return digits
    return None


def score_field(val: Optional[str]) -> int:
    return 95 if val else 0


# -------------------------
# PDF/Text extraction
# -------------------------
def extract_text_from_pdf(pdf_bytes: bytes, max_pages: int = 2) -> str:
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            texts = []
            for page in pdf.pages[:max_pages]:
                t = page.extract_text() or ""
                if t.strip():
                    texts.append(t)
            return "\n".join(texts).strip()
    except Exception:
        return ""


def pdf_to_images_bytes(pdf_bytes: bytes, max_pages: int = 1, dpi: int = 240) -> List[bytes]:
    """
    Render the first page (and a few useful crops) to PNG bytes.

    This is used for scanned PDFs before sending to Gemini Vision.
    Hardened for production (bad uploads, encrypted PDFs) to avoid 500s on Render.
    """
    images: List[bytes] = []

    # Basic sanity checks
    if not pdf_bytes or len(pdf_bytes) < 100:
        raise ValueError("Uploaded file is empty or too small to be a valid PDF.")
    if not pdf_bytes.lstrip().startswith(b"%PDF"):
        raise ValueError("Uploaded file does not look like a PDF (missing %PDF header).")

    # Open PDF
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception as e:
        raise ValueError(f"PyMuPDF failed to open PDF (corrupt/unsupported/encrypted). Details: {e}")

    try:
        # Encrypted PDFs
        if getattr(doc, "is_encrypted", False):
            try:
                doc.authenticate("")  # try empty password
            except Exception:
                pass
            if getattr(doc, "is_encrypted", False):
                raise ValueError("PDF is password-protected. Please upload an unlocked PDF.")

        if getattr(doc, "page_count", 0) <= 0:
            return images

        page = doc.load_page(0)
        zoom = dpi / 72.0
        mat = fitz.Matrix(zoom, zoom)

        rect = page.rect
        # Broad bands (work well across most DBS layouts)
        bands = [
            (0.0, 0.0, 1.0, 0.32),   # header/top
            (0.0, 0.28, 1.0, 0.72),  # middle (name + DOB often here)
            (0.0, 0.68, 1.0, 1.0),   # bottom (dates/notes)
        ]

        # Crops first
        for x0, y0, x1, y1 in bands:
            clip = fitz.Rect(
                rect.x0 + rect.width * x0,
                rect.y0 + rect.height * y0,
                rect.x0 + rect.width * x1,
                rect.y0 + rect.height * y1,
            )
            pix = page.get_pixmap(matrix=mat, clip=clip, alpha=False)
            images.append(pix.tobytes("png"))

        # Full page last
        pix_full = page.get_pixmap(matrix=mat, alpha=False)
        images.append(pix_full.tobytes("png"))

        return images
    finally:
        try:
            doc.close()
        except Exception:
            pass

# -------------------------
# Regex extraction (only works if PDF has a text layer)
# -------------------------
def extract_fields_from_text(text: str) -> Dict[str, Any]:
    t = normalize_ws(text)
    out: Dict[str, Any] = {"certificate_number": None, "surname": None, "dob": None, "issue_date": None}

    m = re.search(r"Certificate\s*Number[:\s]*([0-9\s]{8,20})", t, flags=re.IGNORECASE)
    if m:
        out["certificate_number"] = validate_cert_number(m.group(1))

    m = re.search(r"Surname[:\s]*([A-Z'\-\s]{2,40})", t, flags=re.IGNORECASE)
    if m:
        out["surname"] = normalize_ws(m.group(1)).upper()

    m = re.search(
        r"Date\s*of\s*Birth[:\s]*([0-9]{1,2}\s+[A-Za-z]{3,9}\s+[0-9]{4}|\d{1,2}[\/\.-]\d{1,2}[\/\.-]\d{2,4})",
        t,
        flags=re.IGNORECASE,
    )
    if m:
        parts = parse_uk_date_words(m.group(1)) or parse_ddmmyyyy(m.group(1))
        if parts:
            out["dob"] = {"dd": parts[0], "mm": parts[1], "yyyy": parts[2]}

    # Issue Date ONLY (no Print Date)
    m = re.search(
        r"(Date\s*of\s*Issue|Issue\s*Date)[:\s]*([0-9]{1,2}\s+[A-Za-z]{3,9}\s+[0-9]{4}|\d{1,2}[\/\.-]\d{1,2}[\/\.-]\d{2,4})",
        t,
        flags=re.IGNORECASE,
    )
    if m:
        parts = parse_uk_date_words(m.group(2)) or parse_ddmmyyyy(m.group(2))
        if parts:
            out["issue_date"] = {"dd": parts[0], "mm": parts[1], "yyyy": parts[2]}

    return out


# -------------------------
# Gemini Vision extraction (FAST -> STRONG fallback)
# -------------------------
VISION_PROMPT = """You are extracting ONLY these fields from a UK DBS Enhanced Certificate (page 1):
1) Certificate Number (digits)
2) Applicant Surname (value next to label 'Surname')
3) Date of Birth (label 'Date of Birth')
4) Date of Issue (label 'Date of Issue' or 'Issue Date') — DO NOT use Print Date / Date Printed.

Return STRICT JSON with this schema:
{
  "certificate_number": "string digits or empty",
  "surname": "string or empty",
  "dob": "DD MONTH YYYY or empty",
  "issue_date": "DD MONTH YYYY or empty"
}

Rules:
- If a field is not visible, return empty string for that field.
- Certificate number MUST be digits only.
- Do not guess.
"""


def _parse_json_response(text: str) -> Dict[str, Any]:
    if not text:
        return {}
    text = text.strip()
    text = re.sub(r"^```(json)?", "", text, flags=re.IGNORECASE).strip()
    text = re.sub(r"```$", "", text).strip()
    m = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not m:
        return {}
    try:
        return json.loads(m.group(0))
    except Exception:
        return {}


def gemini_vision_extract_images(images: List[Tuple[bytes, str]]) -> Dict[str, Any]:
    """Vision extraction with FAST -> STRONG fallback.

    Returns:
      {
        certificate_number: str|None,
        surname: str|None,
        dob: {dd,mm,yyyy}|None,
        issue_date: {dd,mm,yyyy}|None,
        _model: str,
      }
    """
    if GEMINI_CLIENT is None or not images:
        return {}

    def _call(model_name: str) -> Dict[str, Any]:
        parts = [types.Part.from_text(text=VISION_PROMPT)]
        for b, mime in images:
            parts.append(types.Part.from_bytes(data=b, mime_type=mime))
        resp = GEMINI_CLIENT.models.generate_content(
            model=model_name,
            contents=[types.Content(role="user", parts=parts)],
        )
        txt = getattr(resp, "text", None) or ""
        data = _parse_json_response(txt)
        if isinstance(data, dict):
            data["_model"] = model_name
        return data if isinstance(data, dict) else {}

    # 1) FAST first
    data: Dict[str, Any] = {}
    try:
        data = _call(GEMINI_MODEL_FAST)
    except Exception:
        data = {}

    # Decide if we need STRONG (any key missing)
    def _is_missing(d: Dict[str, Any]) -> bool:
        if not d:
            return True
        cert_ok = bool(validate_cert_number(str(d.get("certificate_number", "") or "")))
        surname_ok = bool(normalize_ws(str(d.get("surname", "") or "")).strip())
        dob_ok = bool(normalize_ws(str(d.get("dob", "") or "")).strip())
        issue_ok = bool(normalize_ws(str(d.get("issue_date", "") or "")).strip())
        # fallback if any of these are missing
        return not (cert_ok and surname_ok and dob_ok and issue_ok)

    if _is_missing(data):
        try:
            data = _call(GEMINI_MODEL_STRONG)
        except Exception:
            pass

    model_used = str(data.get("_model") or (GEMINI_MODEL_STRONG if _is_missing(data) else GEMINI_MODEL_FAST))

    out: Dict[str, Any] = {
        "certificate_number": validate_cert_number(str(data.get("certificate_number", "") or "")),
        "surname": normalize_ws(str(data.get("surname", "") or "")).upper() or None,
        "dob": None,
        "issue_date": None,
        "_model": model_used,
    }

    dob_parts = (
        parse_uk_date_words(str(data.get("dob", "") or ""))
        or parse_ddmmyyyy(str(data.get("dob", "") or ""))
        or parse_uk_date_words(str(data.get("date_of_birth", "") or ""))
        or parse_ddmmyyyy(str(data.get("date_of_birth", "") or ""))
    )
    if dob_parts:
        out["dob"] = {"dd": dob_parts[0], "mm": dob_parts[1], "yyyy": dob_parts[2]}

    issue_parts = (
        parse_uk_date_words(str(data.get("issue_date", "") or ""))
        or parse_ddmmyyyy(str(data.get("issue_date", "") or ""))
        or parse_uk_date_words(str(data.get("date_of_issue", "") or ""))
        or parse_ddmmyyyy(str(data.get("date_of_issue", "") or ""))
    )
    if issue_parts:
        out["issue_date"] = {"dd": issue_parts[0], "mm": issue_parts[1], "yyyy": issue_parts[2]}

    return out


# -------------------------
# Routes
# -------------------------
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/health")
def health():
    return {
        "gemini_key_present": bool(GEMINI_API_KEY),
        "gemini_import_ok": bool(genai is not None and types is not None),
        "gemini_client_ok": bool(GEMINI_CLIENT is not None),
        "model_fast": GEMINI_MODEL_FAST,
        "model_strong": GEMINI_MODEL_STRONG,
    }



# -------------------------
# Job storage (ephemeral)
# -------------------------
import uuid
import zipfile
import time
from zoneinfo import ZoneInfo

JOBS_ROOT = Path("/tmp") / "dbs_jobs"
JOBS_ROOT.mkdir(parents=True, exist_ok=True)

# In-memory registry: job_id -> {"path": str, "created": float}
JOBS: Dict[str, Dict[str, Any]] = {}

JOB_TTL_SECONDS = 60 * 60  # 1 hour


def _cleanup_jobs() -> None:
    """Best-effort cleanup of old jobs (kept simple for Render)."""
    now = time.time()
    to_delete = []
    for jid, meta in list(JOBS.items()):
        created = float(meta.get("created") or 0)
        if now - created > JOB_TTL_SECONDS:
            to_delete.append(jid)
    for jid in to_delete:
        try:
            job_path = Path(JOBS[jid]["path"])
            if job_path.exists():
                for _ in range(2):
                    try:
                        import shutil
                        shutil.rmtree(job_path, ignore_errors=True)
                        break
                    except Exception:
                        time.sleep(0.1)
        except Exception:
            pass
        JOBS.pop(jid, None)


def _new_job_dir(prefix: str = "job") -> Tuple[str, Path]:
    _cleanup_jobs()
    job_id = str(uuid.uuid4())
    job_dir = JOBS_ROOT / f"{prefix}-{job_id}"
    job_dir.mkdir(parents=True, exist_ok=True)
    JOBS[job_id] = {"path": str(job_dir), "created": time.time()}
    return job_id, job_dir


def _uk_checked_date() -> str:
    # "Checked date" based on UK time.
    dt = datetime.datetime.now(tz=ZoneInfo("Europe/London"))
    return dt.strftime("%d.%m.%Y")


def _safe_filename(name: str, default: str) -> str:
    name = normalize_ws(name).strip()
    if not name:
        name = default
    # Replace illegal filename chars (Windows + URL safety)
    name = re.sub(r'[\\/:*?"<>|]+', "-", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def _calc_date_score(dd: str, mm: str, yyyy: str) -> int:
    dd = (dd or "").strip()
    mm = (mm or "").strip()
    yyyy = (yyyy or "").strip()
    parts = [dd, mm, yyyy]
    filled = sum(1 for p in parts if p)
    if filled == 0:
        return 0
    if filled < 3:
        return int(round((filled / 3) * 100))
    # Basic validation
    if not (dd.isdigit() and mm.isdigit() and yyyy.isdigit()):
        return 60
    d = int(dd)
    m = int(mm)
    y = int(yyyy)
    if not (1 <= d <= 31 and 1 <= m <= 12 and 1900 <= y <= 2100):
        return 70
    return 100


def _verification_score(resp_item: Dict[str, Any]) -> int:
    # Weighted simple score (neutral naming; no AI exposure)
    cert = score_field(resp_item.get("certificate_number", ""))
    sur = score_field(resp_item.get("surname", ""))
    dob = _calc_date_score(resp_item.get("dob_day",""), resp_item.get("dob_month",""), resp_item.get("dob_year",""))
    issue = _calc_date_score(resp_item.get("issue_day",""), resp_item.get("issue_month",""), resp_item.get("issue_year",""))
    # weights: cert 30, sur 25, dob 25, issue 20
    total = (cert*0.30) + (sur*0.25) + (dob*0.25) + (issue*0.20)
    return int(round(total))


# -------------------------
# Extract (single or bulk)
# -------------------------
@app.post("/dbs/extract")
async def dbs_extract(files: List[UploadFile] = File(...)):
    if not files:
        raise HTTPException(status_code=400, detail="No file uploaded.")

    items: List[Dict[str, Any]] = []

    for file in files[:20]:
        content = await file.read()
        filename = (file.filename or "").lower()

        if len(content) > 25 * 1024 * 1024:
            raise HTTPException(status_code=413, detail="File too large. Please upload a PDF/image under 25MB.")

        fields: Dict[str, Any] = {"certificate_number": None, "surname": None, "dob": None, "issue_date": None}
        source: Dict[str, str] = {"certificate_number": "", "surname": "", "dob": "", "issue_date": ""}

        if filename.endswith(".pdf"):
            text = extract_text_from_pdf(content)
            if text and len(text) > 60:
                fields = extract_fields_from_text(text)
                if fields.get("certificate_number"):
                    source["certificate_number"] = "PDF text"
                if fields.get("surname"):
                    source["surname"] = "PDF text"
                if fields.get("dob"):
                    source["dob"] = "PDF text"
                if fields.get("issue_date"):
                    source["issue_date"] = "PDF text"

            if (
                not fields.get("certificate_number")
                or not fields.get("surname")
                or not fields.get("dob")
                or not fields.get("issue_date")
            ):
                try:
                    imgs = pdf_to_images_bytes(content, max_pages=1, dpi=240)
                except Exception as e:
                    raise HTTPException(status_code=400, detail=str(e))
                images = [(b, "image/png") for b in imgs]
                vision = gemini_vision_extract_images(images)
                for k in ["certificate_number", "surname", "dob", "issue_date"]:
                    if not fields.get(k) and vision.get(k):
                        fields[k] = vision.get(k)
                        # Do not mention AI; keep neutral wording.
                        source[k] = "Image scan"
        else:
            is_png = content[:8] == b"\x89PNG\r\n\x1a\n"
            mime = "image/png" if is_png else "image/jpeg"
            vision = gemini_vision_extract_images([(content, mime)])
            fields = {
                "certificate_number": vision.get("certificate_number"),
                "surname": vision.get("surname"),
                "dob": vision.get("dob"),
                "issue_date": vision.get("issue_date"),
            }
            for k in ["certificate_number", "surname", "dob", "issue_date"]:
                if fields.get(k):
                    source[k] = "Image scan"

        dob = fields.get("dob") if isinstance(fields.get("dob"), dict) else {}
        issue = fields.get("issue_date") if isinstance(fields.get("issue_date"), dict) else {}

        resp_item = {
            "original_filename": file.filename or "",
            "certificate_number": fields.get("certificate_number") or "",
            "surname": fields.get("surname") or "",
            "dob_day": (dob.get("dd") or ""),
            "dob_month": (dob.get("mm") or ""),
            "dob_year": (dob.get("yyyy") or ""),
            "issue_day": (issue.get("dd") or ""),
            "issue_month": (issue.get("mm") or ""),
            "issue_year": (issue.get("yyyy") or ""),
            "confidence": {
                "certificate_number": score_field(fields.get("certificate_number") or ""),
                "surname": score_field(fields.get("surname") or ""),
                "dob": _calc_date_score(dob.get("dd") or "", dob.get("mm") or "", dob.get("yyyy") or ""),
                "issue_date": _calc_date_score(issue.get("dd") or "", issue.get("mm") or "", issue.get("yyyy") or ""),
            },
            "source": source,
        }
        resp_item["verification_score"] = _verification_score(resp_item)
        items.append(resp_item)

    return JSONResponse({"items": items})


# -------------------------
# Run DBS (single returns PDF; bulk returns job + links/zip)
# -------------------------
from dbs_runner import run_dbs_check_and_download_pdf


@app.get("/dbs/download/{job_id}/{name}")
async def dbs_download(job_id: str, name: str):
    meta = JOBS.get(job_id)
    if not meta:
        raise HTTPException(status_code=404, detail="Download expired or not found.")
    job_dir = Path(meta["path"])
    file_path = job_dir / name
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found.")
    return FileResponse(path=str(file_path), filename=name, media_type="application/octet-stream")


@app.post("/dbs/run")
async def dbs_run(request: Request):
    payload: Dict[str, Any] = {}
    ctype = (request.headers.get("content-type") or "").lower()
    if "application/json" in ctype:
        try:
            payload = await request.json()
        except Exception:
            payload = {}
    else:
        form = await request.form()
        payload = dict(form)

    organisation_name = (payload.get("organisation_name") or payload.get("org_name") or payload.get("organisation") or "").strip()
    employee_forename = (payload.get("employee_forename") or payload.get("forename") or "").strip()
    employee_surname  = (payload.get("employee_surname")  or payload.get("surname_user") or payload.get("surname") or "").strip()

    if not (organisation_name and employee_forename and employee_surname):
        raise HTTPException(status_code=400, detail="Organisation/Forename/Surname (Step 1) is incomplete.")

    # Bulk mode: items list
    items = payload.get("items")
    if isinstance(items, list) and len(items) > 0:
        download_mode = (payload.get("download_mode") or "zip").lower()  # "zip" or "individual"
        checked_date = _uk_checked_date()
        job_id, job_dir = _new_job_dir(prefix="dbs")

        results: List[Dict[str, Any]] = []
        pdf_names: List[str] = []

        for i, it in enumerate(items[:20], start=1):
            certificate_number = (it.get("certificate_number") or "").strip()
            surname_extracted = (it.get("surname") or it.get("surname_extracted") or it.get("surname_dbs") or "").strip()
            surname_user = (it.get("surname_user") or "").strip()
            applicant_surname = (surname_extracted or surname_user).strip()

            dob_day = str(it.get("dob_day") or "").strip()
            dob_month = str(it.get("dob_month") or "").strip()
            dob_year = str(it.get("dob_year") or "").strip()

            cert = validate_cert_number(certificate_number)
            if not cert:
                results.append({"ok": False, "row": i, "error": "Invalid certificate number.", "filename": ""})
                continue
            if not applicant_surname:
                results.append({"ok": False, "row": i, "error": "Applicant surname is missing.", "filename": ""})
                continue
            if not (dob_day and dob_month and dob_year):
                results.append({"ok": False, "row": i, "error": "DOB is incomplete.", "filename": ""})
                continue

            out_surname = _safe_filename(applicant_surname.upper(), f"SURNAME{i}")
            final_name = f"{out_surname} - DBS Check - {checked_date}.pdf"
            final_name = _safe_filename(final_name, f"DBS-Check-{i}.pdf")

            # Per-candidate subdir to avoid collisions within job
            cand_dir = job_dir / f"c{i:02d}"
            cand_dir.mkdir(parents=True, exist_ok=True)

            result = await anyio.to_thread.run_sync(
                lambda: run_dbs_check_and_download_pdf(
                    organisation_name=organisation_name,
                    employee_forename=employee_forename,
                    employee_surname=employee_surname,
                    certificate_number=cert,
                    applicant_surname=applicant_surname,
                    dob_day=str(dob_day).zfill(2),
                    dob_month=str(dob_month).zfill(2),
                    dob_year=str(dob_year),
                    out_dir=cand_dir,
                    headless=True,
                )
            )

            if not result.get("ok"):
                results.append({
                    "ok": False,
                    "row": i,
                    "error": result.get("error"),
                    "filename": final_name,
                })
                continue

            pdf_path = result.get("pdf_path")
            if not pdf_path or not os.path.exists(pdf_path):
                results.append({
                    "ok": False,
                    "row": i,
                    "error": "Runner succeeded but PDF was not found.",
                    "filename": final_name,
                })
                continue

            # Move into job root with final filename
            final_path = job_dir / final_name
            try:
                if final_path.exists():
                    final_path.unlink()
                os.replace(pdf_path, str(final_path))
            except Exception:
                # fallback to original name
                final_path = Path(pdf_path)
                final_name = final_path.name

            pdf_names.append(final_name)
            results.append({
                "ok": True,
                "row": i,
                "filename": final_name,
                "pdf_url": f"/dbs/download/{job_id}/{final_name}",
            })

        zip_url = ""
        zip_name = ""
        if download_mode == "zip":
            zip_name = _safe_filename(f"DBS_Checks_{checked_date}.zip", "DBS_Checks.zip")
            zip_path = job_dir / zip_name
            with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                for pdfn in pdf_names:
                    fp = job_dir / pdfn
                    if fp.exists():
                        zf.write(fp, arcname=pdfn)
            zip_url = f"/dbs/download/{job_id}/{zip_name}"

        return JSONResponse({
            "job_id": job_id,
            "checked_date": checked_date,
            "download_mode": download_mode,
            "zip_url": zip_url,
            "zip_name": zip_name,
            "results": results,
        })

    # Single mode (existing)
    certificate_number = (payload.get("certificate_number") or "").strip()
    surname_user = (payload.get("surname_user") or payload.get("surname") or "").strip()
    surname_extracted = (payload.get("surname_extracted") or payload.get("surname_dbs") or "").strip()
    applicant_surname = (surname_extracted or surname_user).strip()

    dob_day = (payload.get("dob_day") or payload.get("dob_dd") or "").strip()
    dob_month = (payload.get("dob_month") or payload.get("dob_mm") or "").strip()
    dob_year = (payload.get("dob_year") or payload.get("dob_yyyy") or "").strip()

    cert = validate_cert_number(certificate_number)
    if not cert:
        raise HTTPException(status_code=400, detail="Invalid certificate number.")
    if not applicant_surname:
        raise HTTPException(status_code=400, detail="Applicant surname is missing.")
    if not (dob_day and dob_month and dob_year):
        raise HTTPException(status_code=400, detail="DOB is incomplete.")

    checked_date = _uk_checked_date()
    out_surname = _safe_filename(applicant_surname.upper(), "SURNAME")
    final_name = _safe_filename(f"{out_surname} - DBS Check - {checked_date}.pdf", "DBS-Check.pdf")

    job_id, job_dir = _new_job_dir(prefix="dbs-single")
    result = await anyio.to_thread.run_sync(
        lambda: run_dbs_check_and_download_pdf(
            organisation_name=organisation_name,
            employee_forename=employee_forename,
            employee_surname=employee_surname,
            certificate_number=cert,
            applicant_surname=applicant_surname,
            dob_day=str(dob_day).zfill(2),
            dob_month=str(dob_month).zfill(2),
            dob_year=str(dob_year),
            out_dir=job_dir,
            headless=True,
        )
    )

    if not result.get("ok"):
        detail = {
            "message": "DBS portal run failed.",
            "error": result.get("error"),
            "error_png": result.get("error_png"),
            "trace_path": result.get("trace_path"),
        }
        raise HTTPException(status_code=500, detail=detail)

    pdf_path = result.get("pdf_path")
    if not pdf_path or not os.path.exists(pdf_path):
        raise HTTPException(status_code=500, detail="Runner succeeded but PDF was not found on disk.")

    final_path = job_dir / final_name
    try:
        if final_path.exists():
            final_path.unlink()
        os.replace(pdf_path, str(final_path))
    except Exception:
        final_path = Path(pdf_path)
        final_name = final_path.name

    return FileResponse(path=str(final_path), filename=final_name, media_type="application/pdf")
