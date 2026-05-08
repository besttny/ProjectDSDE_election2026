import os
from dotenv import load_dotenv
load_dotenv()  # loads TYPHOON_API_KEY from .env file

# ── CELL BREAK ──

import argparse
import os, json, hashlib, logging
from pathlib import Path
from collections import defaultdict, Counter

import pandas as pd
import requests
from pypdf import PdfReader

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s | %(message)s")
log = logging.getLogger("nb01")

def _default_project_root() -> Path:
    """Resolve repo root from this script instead of a developer-specific path."""
    return Path(__file__).resolve().parent


# Paths
PROJECT_ROOT = _default_project_root()

GDRIVE_DIR     = PROJECT_ROOT / "data" / "raw" / "pdfs_gdrive"
EXTERNAL_DIR   = PROJECT_ROOT / "data" / "external"

# Constituency metadata
PROVINCE_NAME      = "ชัยภูมิ"
PROVINCE_CODE      = "36"
CONSTITUENCY_NO    = 2
EXPECTED_STATIONS  = 341

# ── CELL BREAK ──

import os, json, re, time, base64, hashlib, logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import requests
from PIL import Image
from bs4 import BeautifulSoup

try:
    import cv2
    HAS_CV2 = True
except ImportError:
    HAS_CV2 = False

try:
    from pdf2image import convert_from_path
except ImportError:
    raise ImportError("pdf2image required: pip install pdf2image && apt-get install poppler-utils")

try:
    from pythainlp.util import text_to_num
    HAS_PYTHAINLP = True
except ImportError:
    HAS_PYTHAINLP = False
    print("⚠️  pythainlp not installed — Thai number cross-check disabled")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s | %(message)s")
log = logging.getLogger("ocr")


# ── CELL BREAK ──

EXTERNAL_DIR   = PROJECT_ROOT / "data" / "external"
IMG_CACHE_DIR  = PROJECT_ROOT / "data" / "raw" / "images"
PROCESSED_DIR  = PROJECT_ROOT / "data" / "processed"
OCR_CACHE_DIR  = PROCESSED_DIR / "ocr_cache"     # 1 file per page (raw OCR text)
PAGE_INDEX_DIR = PROCESSED_DIR / "page_index"    # 1 file per source PDF (parsed headers)
STATION_OUT    = PROCESSED_DIR / "stations_raw"  # 1 JSON per (station × form)

PROVINCE         = "ชัยภูมิ"
CONSTITUENCY_NO  = 2

# Typhoon OCR
TYPHOON_API_KEY = os.getenv("TYPHOON_API_KEY")

TYPHOON_URL = "https://api.opentyphoon.ai/v1/ocr"
TYPHOON_PARAMS = {
    "model": "typhoon-ocr",
    "task_type": "default",
    "max_tokens": 16384,
    "temperature": 0.1,
    "top_p": 0.6,
    "repetition_penalty": 1.2,
}

# ── CELL BREAK ──

stations_df = pd.DataFrame()
manifest_df = pd.DataFrame()
CANDIDATES = {}
PARTIES = {}
TAMBOL_STATIONS = {}


def configure_paths(project_root: Path | str | None = None) -> None:
    """Configure all project-relative paths and create runtime output dirs."""
    global PROJECT_ROOT, GDRIVE_DIR, EXTERNAL_DIR, IMG_CACHE_DIR
    global PROCESSED_DIR, OCR_CACHE_DIR, PAGE_INDEX_DIR, STATION_OUT

    PROJECT_ROOT = Path(project_root).expanduser().resolve() if project_root else _default_project_root()
    GDRIVE_DIR = PROJECT_ROOT / "data" / "raw" / "pdfs_gdrive"
    EXTERNAL_DIR = PROJECT_ROOT / "data" / "external"
    IMG_CACHE_DIR = PROJECT_ROOT / "data" / "raw" / "images"
    PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
    OCR_CACHE_DIR = PROCESSED_DIR / "ocr_cache"
    PAGE_INDEX_DIR = PROCESSED_DIR / "page_index"
    STATION_OUT = PROCESSED_DIR / "stations_raw"

    for p in [EXTERNAL_DIR, IMG_CACHE_DIR, PROCESSED_DIR, OCR_CACHE_DIR, PAGE_INDEX_DIR, STATION_OUT]:
        p.mkdir(parents=True, exist_ok=True)


def load_reference_data() -> None:
    """Load reference CSVs lazily so importing this module has no data dependency."""
    global stations_df, manifest_df, CANDIDATES, PARTIES, TAMBOL_STATIONS

    stations_df = pd.read_csv(EXTERNAL_DIR / "stations.csv", encoding="utf-8-sig")
    candidates_df = pd.read_csv(EXTERNAL_DIR / "candidates.csv", encoding="utf-8-sig")
    parties_df = pd.read_csv(EXTERNAL_DIR / "parties.csv", encoding="utf-8-sig")
    manifest_df = pd.read_csv(EXTERNAL_DIR / "source_manifest.csv", encoding="utf-8-sig")

    CANDIDATES = {int(r["candidate_no"]): (r["candidate_name"], r["party_name"])
                  for _, r in candidates_df.iterrows()}
    PARTIES = {int(r["party_no"]): r["party_name"]
               for _, r in parties_df.iterrows()}
    TAMBOL_STATIONS = {
        t: g.sort_values("station_no")["station_code"].tolist()
        for t, g in stations_df.groupby("subdistrict")
    }


def resolve_project_path(path_value: str | Path | None) -> Path | None:
    """Resolve cached relative paths across POSIX/Windows separators."""
    if path_value is None:
        return None
    path = Path(path_value)
    if path.is_absolute():
        return path
    normalized = Path(*str(path_value).replace("\\", "/").split("/"))
    return PROJECT_ROOT / normalized


# ── CELL BREAK ──

def render_page(pdf_path: Path, page_idx: int, dpi: int = 200) -> Path:
    """Render one PDF page → cached JPG. Idempotent.

    Uses ASCII-only filename for the cached image (hash of source path)
    to avoid Windows/OneDrive issues with Thai characters in filenames.
    """
    # Hash includes the full source path so different PDFs don\'t collide
    key = hashlib.md5(f"{pdf_path}|{page_idx}|{dpi}".encode("utf-8")).hexdigest()[:16]
    out = IMG_CACHE_DIR / f"page_{key}.jpg"
    if out.exists():
        return out

    pages = convert_from_path(str(pdf_path), dpi=dpi,
                              first_page=page_idx + 1, last_page=page_idx + 1)
    if not pages:
        raise IndexError(f"{pdf_path.name} has no page {page_idx}")
    arr = np.array(pages[0])

    if HAS_CV2:
        gray = cv2.cvtColor(arr, cv2.COLOR_RGB2GRAY)
        # Mild denoise — preserve thin Thai strokes
        denoised = cv2.fastNlMeansDenoising(gray, h=8)
        bgr = cv2.cvtColor(denoised, cv2.COLOR_GRAY2BGR)
        # cv2.imwrite doesn\'t handle non-ASCII paths on Windows reliably — use imencode + write
        success, buf = cv2.imencode(".jpg", bgr, [cv2.IMWRITE_JPEG_QUALITY, 92])
        if not success:
            raise RuntimeError(f"cv2.imencode failed for {out}")
        with open(out, "wb") as f:
            f.write(buf.tobytes())
    else:
        Image.fromarray(arr).save(out, "JPEG", quality=92)

    if not out.exists():
        raise FileNotFoundError(f"Render claimed success but {out} doesn\'t exist on disk")

    return out


def count_pdf_pages(pdf_path: Path) -> int:
    """Cheap page count without rendering."""
    from pypdf import PdfReader
    return len(PdfReader(str(pdf_path)).pages)


# ── CELL BREAK ──

class TyphoonError(Exception):
    pass


def _extract_text_from_response(payload: dict) -> str:
    """
    Pull the OCR text out of Typhoon's response. Their actual shape is:
        { "results": [ { "success": True, "message": { "choices": [ {"message": {"content": "..."}} ] } } ] }

    We look at all the obvious places, in order of preference.
    """
    # Real Typhoon shape (results[0].message.choices[0].message.content)
    results = payload.get("results")
    if isinstance(results, list) and results:
        first = results[0]
        if isinstance(first, dict):
            if first.get("success") is False:
                raise TyphoonError(f"Typhoon reported failure: {first.get('error', first)}")
            msg = first.get("message", {})
            if isinstance(msg, dict):
                choices = msg.get("choices")
                if isinstance(choices, list) and choices:
                    content = choices[0].get("message", {}).get("content")
                    if content:
                        return content

    # Other shapes seen in alternative endpoints
    if "text" in payload and isinstance(payload["text"], str):
        return payload["text"]
    if "choices" in payload and isinstance(payload["choices"], list) and payload["choices"]:
        content = payload["choices"][0].get("message", {}).get("content")
        if content:
            return content

    raise TyphoonError(f"Could not extract text from payload: keys={list(payload.keys())[:10]}")


def _typhoon_request(image_path: Path, *, max_retries: int = 3, timeout: int = 60) -> str:
    """One call, with exponential backoff on transient errors."""
    api_key = os.getenv("TYPHOON_API_KEY") or TYPHOON_API_KEY
    if not api_key:
        raise RuntimeError(
            "Set TYPHOON_API_KEY before OCR is needed.\n"
            "  Bash: export TYPHOON_API_KEY='sk-...'\n"
            "  Or put it in a local .env file that is not committed."
        )

    headers = {"Authorization": f"Bearer {api_key}"}
    last_err = None

    for attempt in range(1, max_retries + 1):
        try:
            with open(image_path, "rb") as f:
                resp = requests.post(
                    TYPHOON_URL,
                    headers=headers,
                    files={"file": f},
                    data=TYPHOON_PARAMS.copy(),
                    timeout=timeout,
                )

            if resp.status_code == 200:
                payload = resp.json()
                return _extract_text_from_response(payload)

            if resp.status_code in (429, 500, 502, 503, 504):
                wait = 2 ** attempt
                log.warning(f"  Typhoon HTTP {resp.status_code}, retry {attempt}/{max_retries} in {wait}s")
                time.sleep(wait)
                last_err = f"HTTP {resp.status_code}"
                continue

            raise TyphoonError(f"HTTP {resp.status_code}: {resp.text[:200]}")

        except requests.RequestException as e:
            last_err = str(e)
            time.sleep(2 ** attempt)

    raise TyphoonError(f"Exhausted retries: {last_err}")


def ocr_page(image_path: Path) -> str:
    """OCR with disk cache. Re-running this notebook skips already-OCR\'d pages."""
    cache = OCR_CACHE_DIR / f"{image_path.stem}.txt"
    if cache.exists():
        return cache.read_text(encoding="utf-8")

    raw = _typhoon_request(image_path)
    cache.write_text(raw, encoding="utf-8")
    return raw


# ── CELL BREAK ──

# Thai digit ↔ Arabic digit
THAI_NUM_MAP = str.maketrans("๐๑๒๓๔๕๖๗๘๙", "0123456789")

def thai_to_int(s) -> Optional[int]:
    """Best-effort: extract first integer from a string with Thai or Arabic digits."""
    if s is None:
        return None
    s = str(s).translate(THAI_NUM_MAP)
    m = re.search(r"\d+", s.replace(",", ""))
    return int(m.group(0)) if m else None


# ── Patterns ──────────────────────────────────────────────────────────────────

RX_FORM_PARTY = re.compile(
    r"ส[\.\s]*ส[\.\s]*\s*[5๕]\s*/\s*(?:18|๑๘)\s*\(\s*บช\s*\)",
    re.IGNORECASE
)
RX_FORM_CONSTIT = re.compile(
    r"ส[\.\s]*ส[\.\s]*\s*[5๕]\s*/\s*(?:18|๑๘)(?!\s*\()",
    re.IGNORECASE
)
RX_FORM_516 = re.compile(r"ส[\.\s]*ส[\.\s]*\s*[5๕]\s*/\s*(?:16|๑๖)", re.IGNORECASE)
RX_FORM_517 = re.compile(r"ส[\.\s]*ส[\.\s]*\s*[5๕]\s*/\s*(?:17|๑๗)", re.IGNORECASE)

# "หน่วยเลือกตั้งที่ 1" — accept Thai or Arabic digits, allow leading dots/spaces
RX_STATION_NO = re.compile(r"หน่วยเลือกตั้งที่[\s\.]*([๐-๙\d]+)")

# Introductory paragraph that ONLY appears on page 1 of each station form.
# Used as a fallback first-page marker when the station number is illegible.
RX_FIRST_PAGE_INTRO = re.compile(
    r"บัดนี้\s*คณะกรรมการประจำหน่วยเลือกตั้ง|"
    r"รายงานผลการนับคะแนนสมาชิกสภาผู้แทนราษฎร"
)

# "ตำบล/แขวง/เทศบาล กะฮาด" — capture word after the label
RX_TAMBOL = re.compile(r"ตำบล\s*[/]?\s*(?:แขวง\s*[/]?\s*)?(?:เทศบาล\s*)?([\u0E00-\u0E7F]+?)(?=\s+อำเภอ|\s+เขต|\s*$)")

# "อำเภอ/เขต เนินสง่า"
RX_AMPHOE = re.compile(r"อำเภอ\s*[/]?\s*(?:เขต\s+)?([\u0E00-\u0E7F]+)")

# Page numbering "- 2 -"
RX_PAGE_MARK = re.compile(r"-\s*([๐-๙\d]+)\s*-")


def detect_form_type(text: str) -> Optional[str]:
    """Identify which form this page belongs to from header text."""
    if RX_FORM_PARTY.search(text):
        return "5_18_party"
    if RX_FORM_CONSTIT.search(text):
        return "5_18"
    if RX_FORM_516.search(text):
        return "5_16_party" if "บช" in text or "บัญชีรายชื่อ" in text else "5_16"
    if RX_FORM_517.search(text):
        return "5_17_party" if "บช" in text or "บัญชีรายชื่อ" in text else "5_17"
    return None


# PDF filename → official subdistrict name corrections.
# Some PDFs use common/local names that differ from the official name in stations.csv.
PDF_SUBDISTRICT_ALIASES = {
    "ทุ่งทอง": "ตะโกทอง",   # ต.ทุ่งทอง PDF = ตำบลตะโกทอง (อ.ซับใหญ่)
}


def extract_pdf_location(source_pdf: str) -> dict:
    """
    Derive district and subdistrict from the PDF file path — far more reliable
    than OCR'd tambol/amphoe text.

    Path pattern: .../อำเภอXXX/ต.YYY-[แบ่งเขต|บัญชีรายชื่อ].pdf
    Returns {"district": ..., "subdistrict": ...}; either may be None.

    Multi-tambol files (e.g. "ทต.จัตุรัส ทต.บ้านกอก-...") return subdistrict=None
    because a single subdistrict cannot be determined from the path alone.

    PDF_SUBDISTRICT_ALIASES corrects cases where the PDF uses a common/local name
    that differs from the official name in stations.csv.
    """
    path = Path(source_pdf)
    parent = path.parent.name

    # District: strip "อำเภอ" prefix from the containing folder
    district = re.sub(r'^อำเภอ\s*', '', parent).strip() if 'อำเภอ' in parent else None

    stem = path.stem

    # Multi-tambol: multiple ทต. → can't pin to one subdistrict
    if stem.count('ทต.') > 1:
        return {"district": district, "subdistrict": None}

    subdistrict = None

    # ต.XXX-...
    m = re.match(r'^ต\.([\u0E00-\u0E7F]+)', stem)
    if m:
        subdistrict = m.group(1)

    # ทต.XXX-... (single municipality)
    if subdistrict is None:
        m = re.match(r'^ทต\.([\u0E00-\u0E7F]+)', stem)
        if m:
            subdistrict = m.group(1)

    # Thai-only prefix with no ต./ทต. prefix (e.g. "หนองฉิม-001-แบ่งเขต")
    if subdistrict is None:
        m = re.match(r'^([\u0E00-\u0E7F]+)', stem)
        if m:
            subdistrict = m.group(1)

    # Apply known PDF filename → official name corrections
    if subdistrict and subdistrict in PDF_SUBDISTRICT_ALIASES:
        subdistrict = PDF_SUBDISTRICT_ALIASES[subdistrict]

    return {"district": district, "subdistrict": subdistrict}


def parse_page_header(text: str) -> dict:
    """
    Try to extract page-1 markers from OCR text.
    Returns whatever was found (some keys may be None if missing).

    is_first_page is set True when EITHER:
      (a) station_no is extracted — confident first page with known station
      (b) the introductory paragraph is present but station_no is illegible —
          first page with station_no=None, flagged for review downstream
    """
    out = {
        "form_type":     detect_form_type(text),
        "station_no":    None,
        "tambol":        None,
        "amphoe":        None,
        "page_marker":   None,
        "is_first_page": False,
    }

    m = RX_STATION_NO.search(text)
    if m:
        out["station_no"] = thai_to_int(m.group(1))
        out["is_first_page"] = True
    elif RX_FIRST_PAGE_INTRO.search(text):
        # Station number illegible, but introductory paragraph confirms this is page 1
        out["is_first_page"] = True

    m = RX_TAMBOL.search(text)
    if m:
        out["tambol"] = m.group(1).strip()

    m = RX_AMPHOE.search(text)
    if m:
        out["amphoe"] = m.group(1).strip()

    m = RX_PAGE_MARK.search(text)
    if m:
        out["page_marker"] = thai_to_int(m.group(1))

    return out


# ── CELL BREAK ──

def index_pdf_pages(pdf_path: Path) -> list[dict]:
    """
    Walk pages of a PDF, OCR each, parse header. Returns list of page-dicts:
       {page_idx, image_path, form_type, station_no, tambol, amphoe, is_first_page, ...}
    Caches result to PAGE_INDEX_DIR/<pdf_stem>.json — re-running skips Typhoon calls.
    """
    cache = PAGE_INDEX_DIR / f"{pdf_path.stem}.json"
    if cache.exists():
        return json.loads(cache.read_text(encoding="utf-8"))

    n_pages = count_pdf_pages(pdf_path)
    pages = []
    log.info(f"Indexing {pdf_path.name} ({n_pages} pages)")

    for i in range(n_pages):
        try:
            img = render_page(pdf_path, i)
            text = ocr_page(img)
            hdr = parse_page_header(text)
            pages.append({
                "page_idx":   i,
                "image_path": img.relative_to(PROJECT_ROOT).as_posix(),
                **hdr,
                "ocr_chars":  len(text),
            })
        except Exception as e:
            log.warning(f"  Page {i} failed: {e}")
            pages.append({"page_idx": i, "image_path": None, "form_type": None,
                          "station_no": None, "tambol": None, "amphoe": None,
                          "page_marker": None, "is_first_page": False,
                          "error": str(e)})

    cache.write_text(json.dumps(pages, ensure_ascii=False, indent=2), encoding="utf-8")
    return pages


def chunk_pages_by_station(pages: list[dict]) -> list[dict]:
    """
    Group pages into per-station chunks. A new chunk starts at every is_first_page=True.
    Pages without their own header attach to the most recent chunk.
    """
    chunks = []
    current = None

    for p in pages:
        if p.get("is_first_page"):
            if current:
                chunks.append(current)
            current = {
                "form_type":   p["form_type"],
                "station_no":  p["station_no"],
                "tambol":      p["tambol"],
                "amphoe":      p["amphoe"],
                "page_indices": [p["page_idx"]],
                "image_paths":  [p["image_path"]],
            }
        elif current:
            current["page_indices"].append(p["page_idx"])
            current["image_paths"].append(p["image_path"])
        # else: orphan page before any header — skip silently

    if current:
        chunks.append(current)

    return chunks


# ── CELL BREAK ──

# Header field labels — robust to spacing variation
HEADER_FIELDS = {
    "eligible_voters":   r"จำนวนผู้มีสิทธิ.{0,20}ตามบัญชี",
    "voters_present":    r"(?:จำนวน)?ผู้มีสิทธิ.{0,20}มาแสดงตน",
    "ballots_received":  r"บัตรเลือกตั้งที่ได้รับ",
    "ballots_used":      r"บัตรเลือกตั้งที่ใช้",
    "ballots_good":      r"บัตรดี",
    "ballots_spoiled":   r"บัตรเสีย",
    "ballots_no_vote":   r"บัตรไม่เลือก",
}


def parse_chunk_header(combined_text: str) -> dict:
    """Extract header counts from the combined text of a chunk."""
    out = {}
    for field, label_rx in HEADER_FIELDS.items():
        # Match: <label> ... <digits> [บัตร|คน] ( <thai_word> )
        rx = re.compile(
            label_rx + r".{0,80}?([๐-๙\d,]+)\s*(?:บัตร|คน)?\s*\(([^)]{1,40})\)",
            re.DOTALL,
        )
        m = rx.search(combined_text)
        if m:
            out[field] = {"value": thai_to_int(m.group(1)),
                          "thai_word": m.group(2).strip()}
        else:
            out[field] = {"value": None, "thai_word": None}
    return out


def parse_vote_table(combined_text: str, *, kind: str) -> list[dict]:
    """
    Extract vote rows from all <table> blocks in OCR output.
    kind='candidate' -> entity_no maps to CANDIDATES dict
    kind='party'     -> entity_no maps to PARTIES dict
    """
    soup = BeautifulSoup(combined_text, "html.parser")
    rows = []
    seen = set()

    ref = CANDIDATES if kind == "candidate" else PARTIES

    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            if tr.find("th"):
                continue

            cells = [td.get_text(strip=True) for td in tr.find_all("td")]
            if len(cells) < 2:
                continue

            entity_no = thai_to_int(cells[0])
            if entity_no is None or entity_no not in ref:
                continue

            # Avoid duplicate OCR rows if the same party appears twice
            if entity_no in seen:
                continue
            seen.add(entity_no)

            votes_int = None
            thai_word = None

            for c in cells[1:]:
                n = thai_to_int(c)
                if n is not None and votes_int is None:
                    votes_int = n
                elif re.search(r"[\u0E00-\u0E7F]", c):
                    if not c.startswith(("พรรค", "นาย", "นาง", "นางสาว")):
                        thai_word = c.strip("()")

            rows.append({
                "entity_no": entity_no,
                "votes": votes_int,
                "votes_thai_word": thai_word,
            })

    return rows


# ── CELL BREAK ──

# Empirically-discovered OCR fixes — start empty, grow from your own QA findings
# OCR errors we've observed in Typhoon's output for these specific forms.
# When you find new ones, add them here. The keys must match what OCR actually produces.
KNOWN_OCR_FIXES = {
    # 'X พิเศษ' / 'X ฉลาด' / 'X ปอดี' / 'X ถ้วน' — Typhoon often misreads 'ถ้วน' (final marker)
    # Usually appears after a number, ignored by word-to-int conversion anyway.
    # We strip these in normalize_thai_word() implicitly by not affecting the digit parse.
    "ปอดี": "ถ้วน",
    "พิเศษ": "ถ้วน",
    "ฉลาด": "ถ้วน",
    # 'ทำ' often misread for 'ห้า' in handwriting OCR
    "ทำร้อย": "ห้าร้อย",
}


def normalize_thai_word(w: Optional[str]) -> str:
    if not w:
        return ""
    s = w.strip().strip("()")
    for k, v in KNOWN_OCR_FIXES.items():
        s = s.replace(k, v)
    return s


def thai_word_to_int(w: Optional[str]) -> Optional[int]:
    if not w or not HAS_PYTHAINLP:
        return None
    try:
        cleaned_word = normalize_thai_word(w)
        num_result = text_to_num(cleaned_word)
        
        # FIX: If text_to_num returns a list, extract the first item
        if isinstance(num_result, list):
            if not num_result: # Handle empty lists
                return None
            num_result = num_result[0]
            
        return int(num_result)
        
    # ADDED TypeError to prevent the script from completely crashing in the future
    except (ValueError, KeyError, IndexError, TypeError):
        return None


def cross_check(value: Optional[int], thai_word: Optional[str]) -> tuple[bool, str]:
    """Return (ok, reason). Both missing is OK (=both_missing, treated as info not error)."""
    if value is None and not thai_word:
        return True, "both_missing"
    if value is None:
        return False, "digit_missing"
    if not thai_word:
        return False, "word_missing"
    converted = thai_word_to_int(thai_word)
    if converted is None:
        return False, f"unparseable_word:{thai_word}"
    if converted != value:
        return False, f"mismatch:{value}!={converted}({thai_word})"
    return True, ""


def validate_chunk(header: dict, votes: list[dict]) -> tuple[list[str], str]:
    """Run all checks. Returns (flag_list, status)."""
    flags = []

    # Digit↔word for header fields
    for field, rec in header.items():
        ok, reason = cross_check(rec["value"], rec["thai_word"])
        if not ok:
            flags.append(f"hdr_{field}:{reason}")

    H = {k: v["value"] for k, v in header.items()}

    # ballots_used == good + spoiled + no_vote
    if all(H.get(k) is not None for k in ("ballots_used", "ballots_good", "ballots_spoiled", "ballots_no_vote")):
        s = H["ballots_good"] + H["ballots_spoiled"] + H["ballots_no_vote"]
        if s != H["ballots_used"]:
            flags.append(f"sum_used!=g+s+nv ({H['ballots_used']}!={s})")

    # sum(votes) == ballots_good
    if H.get("ballots_good") is not None:
        total = sum((v.get("votes") or 0) for v in votes)
        if total != H["ballots_good"]:
            flags.append(f"sum_votes!=good ({total}!={H['ballots_good']})")

    # Per-vote digit↔word
    for v in votes:
        ok, _ = cross_check(v.get("votes"), v.get("votes_thai_word"))
        v["needs_review"] = not ok

    status = "ok" if not flags else "needs_review"
    return flags, status


# ── CELL BREAK ──

def lookup_station_metadata(form_type: str, station_no, tambol: Optional[str],
                             source_pdf: Optional[str] = None) -> dict:
    """Match (station_no, tambol) → station_code from stations.csv.

    Resolution order (most-to-least reliable):
      1. station_no + subdistrict from source PDF path  ← new, most reliable
      2. station_no + OCR tambol (often garbled by Typhoon)
      3. station_no + district from PDF path (multi-tambol files land here)
      4. First candidate by station_no alone (last resort — marked with '?')

    Root cause of the old "always บ้านเขว้า" bug:
      The old fallback used `iloc[0]` on the full dataframe.  Since stations.csv
      is sorted with อำเภอบ้านเขว้า first, every failed tambol-match returned a
      บ้านเขว้า row.  The fix is to anchor on the PDF path instead.
    """
    if form_type not in ("5_18", "5_18_party") or station_no is None:
        return {"station_code": None, "district": None, "subdistrict": None}

    # Reliable location extracted from the PDF file path
    pdf_loc = extract_pdf_location(source_pdf) if source_pdf else {"district": None, "subdistrict": None}

    candidates = stations_df[stations_df["station_no"].astype(int) == int(station_no)]
    if candidates.empty:
        return {"station_code": None, "district": None, "subdistrict": None}

    def _pick(df, uncertain=False):
        r = df.iloc[0]
        mark = "?" if (uncertain or len(df) > 1) else ""
        return {"station_code": str(r["station_code"]) + mark,
                "district":     r["district"],
                "subdistrict":  r["subdistrict"]}

    # ── 1. station_no + PDF subdistrict ──────────────────────────────────────
    if pdf_loc.get("subdistrict"):
        m = candidates[candidates["subdistrict"] == pdf_loc["subdistrict"]]
        if not m.empty:
            return _pick(m)

    # ── 2. station_no + OCR tambol ────────────────────────────────────────────
    if tambol:
        m = candidates[candidates["subdistrict"] == tambol]
        if len(m) == 1:
            return _pick(m)

    # ── 3. station_no + PDF district (multi-tambol files) ────────────────────
    if pdf_loc.get("district"):
        m = candidates[candidates["district"] == pdf_loc["district"]]
        if not m.empty:
            # OCR tambol as tiebreaker when multiple candidates in the same district
            if tambol and len(m) > 1:
                m2 = m[m["subdistrict"] == tambol]
                if len(m2) == 1:
                    return _pick(m2)
            return _pick(m)

    # ── 4. Last resort ────────────────────────────────────────────────────────
    return _pick(candidates, uncertain=True)


def process_chunk(chunk: dict, source_pdf: str) -> dict:
    """
    Take an indexed chunk, fetch all its OCR'd text, parse + validate.
    Returns dict: {station: {...}, votes: [...], error: ...}
    """
    form_type = chunk["form_type"]
    if form_type is None:
        return {"error": "form_type_unknown", "station": None, "votes": []}

    # Combine OCR text from all pages of this chunk
    page_texts = []
    for img_rel in chunk["image_paths"]:
        if img_rel is None:
            continue
        img = resolve_project_path(img_rel)
        if img is None:
            continue
        try:
            page_texts.append(ocr_page(img))
        except Exception as e:
            log.warning(f"  OCR retrieval failed for {img}: {e}")
    combined = "\n".join(page_texts)

    # Resolve station identity — pass source_pdf so lookup can use path-derived location
    meta = lookup_station_metadata(form_type, chunk["station_no"], chunk["tambol"], source_pdf)

    # Parse body
    header = parse_chunk_header(combined)
    kind = "party" if form_type.endswith("_party") else "candidate"
    votes_raw = parse_vote_table(combined, kind=kind)
    flags, status = validate_chunk(header, votes_raw)

    # If station_no couldn't be read from the form, flag it explicitly
    if chunk["station_no"] is None:
        flags = ["station_no_unreadable"] + flags
        status = "needs_review"

    station_record = {
        "province":         PROVINCE,
        "constituency_no":  CONSTITUENCY_NO,
        "form_type":        form_type,
        "station_code":     meta["station_code"],
        "station_no":       chunk["station_no"],
        "district":         meta["district"],
        "subdistrict":      meta["subdistrict"] or chunk["tambol"],
        **{k: v["value"] for k, v in header.items()},
        "source_pdf":       source_pdf,
        "source_pages":     ",".join(str(p) for p in chunk["page_indices"]),
        "ocr_status":       status,
        "validation_flags": ";".join(flags),
    }

    vote_rows = []
    ref = CANDIDATES if kind == "candidate" else PARTIES
    for v in votes_raw:
        no = v["entity_no"]
        if kind == "candidate":
            name, party = ref.get(no, ("?", "?"))
        else:
            name, party = ref.get(no, "?"), None
        vote_rows.append({
            "province":         PROVINCE,
            "constituency_no":  CONSTITUENCY_NO,
            "form_type":        form_type,
            "station_code":     meta["station_code"],
            "station_no":       chunk["station_no"],
            "district":         meta["district"],
            "subdistrict":      meta["subdistrict"] or chunk["tambol"],
            "entity_kind":      kind,
            "entity_no":        no,
            "entity_name":      name,
            "party_name":       party,
            "votes":            v.get("votes"),
            "votes_thai_word":  v.get("votes_thai_word"),
            "needs_review":     v.get("needs_review", False),
        })

    return {"station": station_record, "votes": vote_rows, "error": None}


# ── CELL BREAK ──

def chunk_checkpoint_path(source_pdf_rel: str, station_no, form_type: str,
                           first_page_idx: int = 0) -> Path:
    safe = source_pdf_rel.replace("/", "__").replace("\\", "__").replace(".pdf", "").replace(".PDF", "")
    # Use page index as surrogate key when station_no is unreadable
    st_label = str(station_no) if station_no is not None else f"unkn{first_page_idx}"
    return STATION_OUT / f"{safe}__{form_type}__st{st_label}.json"


def run_batch(*, force: bool = False, limit_pdfs: Optional[int] = None,
              filter_kind: Optional[str] = None):
    """
    Process every unique source PDF in the manifest.
       force=True       — re-process even if checkpoint exists
       limit_pdfs=N     — only do first N PDFs (smoke test)
       filter_kind=...  — only PDFs of this source_kind ('pure', 'mixed', 'constituency')

    Chunks with station_no=None (illegible station number) are now saved with a
    placeholder key (unkn<page_idx>) and flagged 'station_no_unreadable' so they
    appear in the QA report rather than being silently dropped.
    """
    configure_paths(PROJECT_ROOT)
    load_reference_data()

    # Unique source PDFs (drop missing)
    sources = manifest_df.dropna(subset=["source_pdf"]).copy()
    if filter_kind:
        sources = sources[sources["source_kind"] == filter_kind]
    unique_pdfs = sources["source_pdf"].unique()
    if limit_pdfs:
        unique_pdfs = unique_pdfs[:limit_pdfs]

    n_pdfs = len(unique_pdfs)
    n_chunks_done = 0
    n_chunks_failed = 0

    for i, src_rel in enumerate(unique_pdfs, 1):
        pdf = PROJECT_ROOT / src_rel
        log.info(f"[{i}/{n_pdfs}] {src_rel}")

        if not pdf.exists():
            log.warning(f"  Missing: {pdf}")
            continue

        try:
            pages = index_pdf_pages(pdf)
            chunks = chunk_pages_by_station(pages)
            log.info(f"  Found {len(chunks)} chunks across {len(pages)} pages")
        except Exception as e:
            log.exception(f"  Indexing failed: {e}")
            continue

        for chunk in chunks:
            # Skip chunks with no form type (can't determine what form this is)
            if not chunk.get("form_type"):
                continue
            # station_no=None is now allowed — saved with unknN key and flagged for review
            st = chunk.get("station_no")
            first_page = chunk["page_indices"][0] if chunk.get("page_indices") else 0
            cp = chunk_checkpoint_path(src_rel, st, chunk["form_type"], first_page)
            if cp.exists() and not force:
                continue
            try:
                result = process_chunk(chunk, src_rel)
                cp.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
                if result.get("error"):
                    n_chunks_failed += 1
                else:
                    n_chunks_done += 1
            except Exception as e:
                log.exception(f"  Chunk failed: station={st} form={chunk.get('form_type')}")
                n_chunks_failed += 1

    log.info(f"Batch done. Processed {n_chunks_done} chunks, {n_chunks_failed} failed.")


# ── CELL BREAK ──

# Smoke test example:
#   python run_batch.py run --limit-pdfs 1 --force
#
# Full run:
#   python run_batch.py run --force


# ── CELL BREAK ──

def aggregate_checkpoints():
    station_rows, vote_rows = [], []
    n_files = 0
    for cp in STATION_OUT.glob("*.json"):
        try:
            data = json.loads(cp.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            log.warning(f"Bad JSON: {cp.name}")
            continue
        n_files += 1
        if data.get("station"):
            station_rows.append(data["station"])
        vote_rows.extend(data.get("votes", []))

    log.info(f"Aggregated {n_files} checkpoint files → {len(station_rows)} station records, {len(vote_rows)} vote rows")

    stations = pd.DataFrame(station_rows)
    votes    = pd.DataFrame(vote_rows)

    # Type cleanup
    for c in ["constituency_no", "station_no", "eligible_voters", "voters_present",
              "ballots_received", "ballots_used", "ballots_good",
              "ballots_spoiled", "ballots_no_vote"]:
        if c in stations.columns:
            stations[c] = pd.to_numeric(stations[c], errors="coerce").astype("Int64")

    if "votes" in votes.columns:
        votes["votes"]     = pd.to_numeric(votes["votes"],     errors="coerce").astype("Int64")
        votes["entity_no"] = pd.to_numeric(votes["entity_no"], errors="coerce").astype("Int64")

    # Save
    stations.to_parquet(PROCESSED_DIR / "stations.parquet", index=False)
    stations.to_csv(PROCESSED_DIR / "stations.csv", index=False, encoding="utf-8-sig")
    votes.to_parquet(PROCESSED_DIR / "votes.parquet", index=False)
    votes.to_csv(PROCESSED_DIR / "votes.csv", index=False, encoding="utf-8-sig")

    print(f"Wrote stations.parquet ({len(stations)} rows), votes.parquet ({len(votes)} rows)")
    return stations, votes


# ── CELL BREAK ──

def qa_report():
    if not (PROCESSED_DIR / "stations.parquet").exists():
        print("Run aggregate_checkpoints() first.")
        return

    s = pd.read_parquet(PROCESSED_DIR / "stations.parquet")
    v = pd.read_parquet(PROCESSED_DIR / "votes.parquet")

    print("OCR status:")
    print(s["ocr_status"].value_counts().to_string())
    print()

    print("By form type × status:")
    print(s.groupby(["form_type", "ocr_status"]).size().unstack(fill_value=0).to_string())
    print()

    flags = s["validation_flags"].dropna().str.split(";").explode()
    flags = flags[flags.str.len() > 0]
    if len(flags):
        print(f"Top 15 validation flags ({len(flags)} total):")
        # Just take the prefix before the first ":"
        flag_types = flags.str.split(":").str[0]
        print(flag_types.value_counts().head(15).to_string())
        print()

    # Compare totals against ground truth
    ref = json.loads((EXTERNAL_DIR / "reference_constituency.json").read_text(encoding="utf-8"))
    ref_total_constit = ref["constituency"]["total_candidate_votes"]
    ref_total_party   = ref["party_list"]["total_party_votes"]

    our_total_constit = v[v["form_type"] == "5_18"]["votes"].sum()
    our_total_party   = v[v["form_type"] == "5_18_party"]["votes"].sum()

    print("=" * 60)
    print("GROUND TRUTH COMPARISON")
    print("=" * 60)
    print(f"Constituency votes:")
    print(f"  Reference (PBS):  {ref_total_constit:>10,}")
    print(f"  Our OCR:          {our_total_constit:>10,}")
    print(f"  Coverage:         {our_total_constit/ref_total_constit*100:.1f}%")
    print()
    print(f"Party-list votes:")
    print(f"  Reference (PBS):  {ref_total_party:>10,}")
    print(f"  Our OCR:          {our_total_party:>10,}")
    print(f"  Coverage:         {our_total_party/ref_total_party*100:.1f}%")
    print()

    # Save needs-review subset for manual fixing
    needs_review = s[s["ocr_status"] != "ok"]
    needs_review.to_csv(PROCESSED_DIR / "qa_needs_review.csv", index=False, encoding="utf-8-sig")
    print(f"Stations needing review: {len(needs_review)} -> qa_needs_review.csv")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="OCR, aggregate, and QA election PDF extraction outputs."
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        default=_default_project_root(),
        help="Project root. Defaults to the directory containing run_batch.py.",
    )

    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Run OCR extraction over source PDFs.")
    run_parser.add_argument("--force", action="store_true", help="Reprocess existing checkpoints.")
    run_parser.add_argument("--limit-pdfs", type=int, default=None, help="Process only the first N PDFs.")
    run_parser.add_argument("--filter-kind", default=None, help="Only process one source_kind value.")

    subparsers.add_parser("aggregate", help="Aggregate checkpoint JSON files into processed CSV/parquet.")
    subparsers.add_parser("qa", help="Print QA report from processed parquet outputs.")

    all_parser = subparsers.add_parser("all", help="Run OCR, aggregate, and QA report.")
    all_parser.add_argument("--force", action="store_true", help="Reprocess existing checkpoints.")
    all_parser.add_argument("--limit-pdfs", type=int, default=None, help="Process only the first N PDFs.")
    all_parser.add_argument("--filter-kind", default=None, help="Only process one source_kind value.")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return 0
    command = args.command

    configure_paths(args.project_root)
    print(f"Project root:  {PROJECT_ROOT}")
    print(f"OCR cache:     {OCR_CACHE_DIR}")
    print(f"Page index:    {PAGE_INDEX_DIR}")
    print(f"Station out:   {STATION_OUT}")

    if command in {"run", "all"}:
        run_batch(force=args.force, limit_pdfs=args.limit_pdfs, filter_kind=args.filter_kind)

    if command in {"aggregate", "all"}:
        aggregate_checkpoints()

    if command == "qa":
        qa_report()
    elif command == "all":
        qa_report()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
