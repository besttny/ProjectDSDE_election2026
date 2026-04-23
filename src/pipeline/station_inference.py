from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pandas as pd

from src.pipeline.config import ProjectConfig
from src.pipeline.manifest import ManifestEntry, load_manifest

STATION_FORMS = {"5_18", "5_18_partylist"}

PARTY_HEADER_MARKERS = (
    "แบบบัญชีรายชื่อ",
    "แบบัญชีรายชื่อ",
    "แบบบัญชรายชื่อ",
    "แบบบัญซีรายชื่อ",
    "รายงานผลการนับคะแนนสมาชิกสภาผู้แทนราษฎรแบบบัญชีรายชื่อ",
    "๕/๑๘(บช)",
    "๕/๑๘ (บช)",
    "๕/18(บช)",
    "๕/18 (บช)",
    "๕๑๘ (บช)",
    "518 (บช)",
    "5/18 (บช)",
    "๕๑๘(บช)",
    "518(บช)",
    "5/18(บช)",
)

PARTY_START_MARKERS = (
    "รายงานผลการนับคะแนนสมาชิกสภาผู้แทนราษฎรแบบบัญชีรายชื่อ",
    "รายงานผลการนับคะแนน",
    "ตามที่ได้มีพระราชกฤษฎีกา",
    "ได้กำหนดให้วันที่",
)

CONSTITUENCY_HEADER_MARKERS = (
    "แบบแบ่งเขต",
    "แบ่งเขต",
    "ส.ส. ๕/๑๘",
    "ส.ส.๕/๑๘",
    "ส.ส. ๕/18",
    "ส.ส.๕/18",
    "ส.ส. 5/18",
    "ส.ส.5/18",
    "ส.ส. 518",
    "ส.ส.518",
)

CONSTITUENCY_START_MARKERS = (
    "รายงานผลการนับคะแนน",
    "ตามที่ได้มีพระราชกฤษฎีกา",
    "บัดนี้ คณะกรรมการ",
    "จำนวนผู้มีสิทธิเลือกตั้ง",
    "จำนวนผู้มิสิทธิเลือกตั้ง",
    "จำนวนบัตรเลือกตั้ง",
    "จำนวนคะแนนที่ผู้สมัคร",
    "ผู้สมัครแต่ละคนได้รับ",
    "หน่วยเลือกตั้งดังกล่าวดังนี้",
)

SIGNATURE_MARKERS = (
    "ประกาศ ณ",
    "ประธานกรรมการ",
    "กรรมการประจำหน่วย",
    "ลงชื่อ",
)

CONSTITUENCY_EXCLUSIVE_MARKERS = (
    "แบบแบ่งเขต",
    "แบบแป่งเขต",
    "แบบแบ่งเขด",
    "แบบแบ่งเขตเลือกตั้ง",
    "แบบแบ่งเขดเลือกตั้ง",
    "แบบแบ่งเขตเลือุกตั้ง",
    "รายงานผลการนับคะแนนสมาชิกสภาผู้แทนราษฎรแบบแบ่งเขต",
)


def _page_number(path: Path) -> int:
    match = re.search(r"_page_(\d+)", path.stem)
    return int(match.group(1)) if match else 0


def _read_header_text(path: Path) -> str:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ""
    lines = payload.get("lines", [])
    return " ".join(str(line.get("text", "")) for line in lines[:18])


def _compact_text(text: str) -> str:
    return re.sub(r"\s+", "", text)


def _is_party_header(text: str) -> bool:
    compact = _compact_text(text)
    has_constituency_header = any(
        marker in text or _compact_text(marker) in compact
        for marker in CONSTITUENCY_EXCLUSIVE_MARKERS
    )
    has_explicit_party_form = any(
        marker in text or _compact_text(marker) in compact
        for marker in PARTY_HEADER_MARKERS
        if "บช" in marker or "บซ" in marker or "แบบบัญ" in marker or "แบบัญ" in marker
    )
    if has_constituency_header and not has_explicit_party_form:
        return False
    if any(marker in text or _compact_text(marker) in compact for marker in PARTY_HEADER_MARKERS):
        return True
    if (
        ("หมายเลขของบัญชีรายชื่อ" in text or "หมายเลขของบัญซีรายชื่อ" in text)
        and "พรรคการเมือง" in text
    ):
        return True
    if "บัญชีรายชื่อ" in text and "พรรคการเมือง" in text and "ผู้สมัคร" not in text:
        return True
    return False


def _is_party_start_page(text: str) -> bool:
    if not _is_party_header(text):
        return False
    compact = _compact_text(text)
    return any(
        marker in text or _compact_text(marker) in compact
        for marker in PARTY_START_MARKERS
    )


def _is_constituency_header(text: str) -> bool:
    compact = _compact_text(text)
    if _is_party_header(text):
        return False
    return any(
        marker in text or _compact_text(marker) in compact
        for marker in CONSTITUENCY_HEADER_MARKERS
    )


def _is_constituency_start_page(text: str) -> bool:
    if not _is_constituency_header(text):
        return False
    if any(marker in text for marker in CONSTITUENCY_START_MARKERS):
        return True
    # Some OCR payloads drop the "report" wording but keep the first-page
    # summary fields. Signature pages usually only contain announce/signature
    # text, so keep them attached to the current station.
    has_signature_text = any(marker in text for marker in SIGNATURE_MARKERS)
    has_summary_text = "จำนวน" in text and ("บัตร" in text or "สิทธิ" in text)
    return has_summary_text and not has_signature_text


def _page_kind(text: str) -> str:
    if _is_party_header(text):
        return "5_18_partylist"
    if _is_constituency_header(text):
        return "5_18"
    return ""


def _page_texts(raw_dir: Path) -> dict[int, str]:
    return {
        _page_number(path): _read_header_text(path)
        for path in sorted(raw_dir.glob("*.json"), key=_page_number)
        if _page_number(path)
    }


def _first_party_page(raw_dir: Path) -> int | None:
    for path in sorted(raw_dir.glob("*.json"), key=_page_number):
        if _is_party_header(_read_header_text(path)):
            return _page_number(path)
    return None


def _raw_dir(config: ProjectConfig, entry: ManifestEntry) -> Path:
    return config.path("raw_ocr_dir") / entry.form_type / entry.file_path.stem


def _source_name(value: object) -> str:
    text = "" if pd.isna(value) else str(value).strip()
    return Path(text).name if text else ""


def _clean_subdistrict_from_stem(stem: str) -> str:
    name = re.split(r"-|_", stem, maxsplit=1)[0].strip()
    name = re.sub(r"^(ต\.|ทต\.)", "", name).strip()
    name = re.sub(r"\s+", " ", name)
    return name


def _district_from_path(path: Path) -> str:
    for part in reversed(path.parts):
        if part.startswith("อำเภอ"):
            return part.replace("อำเภอ", "", 1).strip()
    return ""


def _next_known_kind(kinds: dict[int, str], page: int) -> str:
    for future_page in sorted(candidate for candidate in kinds if candidate > page):
        if kinds[future_page]:
            return kinds[future_page]
    return ""


def _previous_known_kind(kinds: dict[int, str], page: int) -> str:
    for previous_page in sorted((candidate for candidate in kinds if candidate < page), reverse=True):
        if kinds[previous_page]:
            return kinds[previous_page]
    return ""


def _resolve_unknown_auto_pages(
    pages: list[int],
    kinds: dict[int, str],
) -> dict[int, str]:
    resolved = kinds.copy()
    for page in pages:
        if resolved.get(page):
            continue
        previous_kind = _previous_known_kind(resolved, page)
        next_kind = _next_known_kind(resolved, page)
        if previous_kind == next_kind and previous_kind:
            resolved[page] = previous_kind
        elif previous_kind == "5_18" and next_kind == "5_18_partylist":
            resolved[page] = "5_18"
        elif previous_kind == "5_18_partylist" and next_kind == "5_18":
            resolved[page] = "5_18_partylist"
        elif not previous_kind and next_kind == "5_18_partylist":
            resolved[page] = "5_18"
        elif previous_kind:
            resolved[page] = previous_kind
        elif next_kind:
            resolved[page] = next_kind
    return resolved


def _fixed_form_assignments(
    pages: list[int],
    form_type: str,
    *,
    pages_per_station: int,
) -> dict[int, dict[str, int | str]]:
    assignments: dict[int, dict[str, int | str]] = {}
    current_station = 0
    page_in_station = 0
    for page in pages:
        if page_in_station == 0:
            current_station += 1
        assignments[page] = {"form_type": form_type, "station_group_local": current_station}
        page_in_station = (page_in_station + 1) % pages_per_station
    return assignments


def _auto_page_assignments(raw_dir: Path) -> dict[int, dict[str, int | str]]:
    texts = _page_texts(raw_dir)
    pages = sorted(texts)
    assignments: dict[int, dict[str, int | str]] = {}
    first_party = _first_party_page(raw_dir)
    if first_party is not None and first_party > 6:
        current_station = 0
        for page in [candidate for candidate in pages if candidate < first_party]:
            if current_station == 0 or _is_constituency_start_page(texts[page]):
                current_station += 1
            assignments[page] = {
                "form_type": "5_18",
                "station_group_local": current_station,
            }

        for index, page in enumerate([candidate for candidate in pages if candidate >= first_party]):
            assignments[page] = {
                "form_type": "5_18_partylist",
                "station_group_local": (index // 4) + 1,
            }
        return assignments

    kinds = _resolve_unknown_auto_pages(
        pages,
        {page: _page_kind(texts[page]) for page in pages},
    )
    start_pages: list[int] = []
    previous_kind = ""
    for page in pages:
        kind = kinds.get(page)
        if _is_constituency_start_page(texts[page]) or (
            kind == "5_18" and previous_kind == "5_18_partylist"
        ):
            start_pages.append(page)
        if kind:
            previous_kind = kind
    if not start_pages and pages:
        start_pages = [pages[0]]

    # Pages before the first detected start page are usually a weak OCR read of
    # the first constituency page. Keep them in station 1 rather than dropping
    # them or trusting the printed station number.
    if pages and pages[0] < start_pages[0]:
        start_pages = [pages[0], *start_pages]

    stations_with_preassigned_party: set[int] = set()
    for station_index, start_page in enumerate(start_pages, start=1):
        next_start = start_pages[station_index] if station_index < len(start_pages) else None
        segment_pages = [
            page
            for page in pages
            if start_page <= page and (next_start is None or page < next_start)
        ]
        if not segment_pages:
            continue

        party_start = next(
            (page for page in segment_pages[1:] if _is_party_header(texts[page])),
            None,
        )
        if party_start is None and len(segment_pages) > 2:
            # ECT 5/18 constituency pages are normally a report page plus a
            # signature page. When OCR misses the party-list marker, the third
            # page of a station segment is the safest party-list fallback.
            party_start = segment_pages[2]

        next_station_party_start = None
        if next_start is not None:
            party_start_pages = [
                page for page in segment_pages[1:] if _is_party_start_page(texts[page])
            ]
            if len(party_start_pages) > 1:
                # Some scanned bundles place the party-list pages for the next
                # station immediately before that station's constituency pages.
                # Without this lookahead, pages such as local station 9's
                # party-list block are swallowed by the previous station.
                next_station_party_start = party_start_pages[-1]
            elif station_index in stations_with_preassigned_party and party_start is not None:
                # If the previous segment already gave this station a trailing
                # party-list block, the next party block in this segment belongs
                # to the following station. This handles short runs where the
                # bundle stays in party-before-constituency order for one more
                # station before returning to the normal order.
                next_station_party_start = party_start
        if next_station_party_start is not None:
            stations_with_preassigned_party.add(station_index + 1)

        for page in segment_pages:
            station_group_local = station_index
            if next_station_party_start is not None and page >= next_station_party_start:
                station_group_local = station_index + 1
            form_type = "5_18_partylist" if party_start is not None and page >= party_start else "5_18"
            assignments[page] = {
                "form_type": form_type,
                "station_group_local": station_group_local,
            }
    return assignments


def _page_assignments(entry: ManifestEntry, raw_dir: Path) -> dict[int, dict[str, int | str]]:
    pages = sorted(_page_number(path) for path in raw_dir.glob("*.json") if _page_number(path))
    if not pages:
        return {}

    if entry.form_type == "5_18":
        return _fixed_form_assignments(pages, "5_18", pages_per_station=2)

    if entry.form_type == "5_18_partylist":
        return _fixed_form_assignments(pages, "5_18_partylist", pages_per_station=4)

    if entry.form_type != "5_18_auto":
        return {}

    return _auto_page_assignments(raw_dir)


def build_station_page_map(config: ProjectConfig) -> dict[tuple[str, str, int], dict[str, Any]]:
    """Infer stable station ids from source PDF page position.

    ECT 5/18 files are fixed-layout documents. OCR often reads the printed
    station number as a local "1" or misses it entirely, so the downstream
    schema uses a deterministic station id inferred from manifest order,
    source PDF, and page group instead of trusting OCR text.
    """

    station_offsets = {"5_18": 0, "5_18_partylist": 0}
    page_map: dict[tuple[str, str, int], dict[str, Any]] = {}
    for entry in load_manifest(config):
        raw_dir = _raw_dir(config, entry)
        assignments = _page_assignments(entry, raw_dir)
        if not assignments:
            continue

        max_local_by_form = {
            form_type: max(
                int(info["station_group_local"])
                for info in assignments.values()
                if info["form_type"] == form_type
            )
            for form_type in STATION_FORMS
            if any(info["form_type"] == form_type for info in assignments.values())
        }
        source_name = entry.file_path.name
        district = _district_from_path(entry.file_path)
        subdistrict = _clean_subdistrict_from_stem(entry.file_path.stem)

        for page, info in assignments.items():
            form_type = str(info["form_type"])
            local_station = int(info["station_group_local"])
            global_station = station_offsets[form_type] + local_station
            page_map[(form_type, source_name, page)] = {
                "polling_station_no": global_station,
                "form_type": form_type,
                "vote_type": "party_list" if form_type == "5_18_partylist" else "constituency",
                "district": district,
                "subdistrict": subdistrict,
                "station_group_local": local_station,
            }
            # Also keep a page-only key so rows whose form type was misread by
            # the parser can be corrected from the page header classification.
            page_map[("__page__", source_name, page)] = page_map[(form_type, source_name, page)]

        for form_type, max_local in max_local_by_form.items():
            station_offsets[form_type] += max_local

    return page_map


def apply_station_inference(df: pd.DataFrame, config: ProjectConfig) -> pd.DataFrame:
    if df.empty:
        return df
    page_map = build_station_page_map(config)
    if not page_map:
        return df

    corrected = df.copy()
    for index, row in corrected.iterrows():
        form_type = str(row.get("form_type", "")).strip()
        if form_type not in STATION_FORMS:
            continue
        source_page = pd.to_numeric(pd.Series([row.get("source_page", "")]), errors="coerce").iloc[0]
        if pd.isna(source_page):
            continue
        source_name = _source_name(row.get("source_pdf", ""))
        page = int(source_page)
        inferred = page_map.get((form_type, source_name, page)) or page_map.get(
            ("__page__", source_name, page)
        )
        if not inferred:
            continue
        corrected.at[index, "form_type"] = inferred["form_type"]
        corrected.at[index, "vote_type"] = inferred["vote_type"]
        corrected.at[index, "polling_station_no"] = inferred["polling_station_no"]
        if inferred["district"]:
            corrected.at[index, "district"] = inferred["district"]
        if inferred["subdistrict"]:
            corrected.at[index, "subdistrict"] = inferred["subdistrict"]
    return corrected
