from __future__ import annotations

from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
import re

import pandas as pd


@dataclass(frozen=True)
class MatchResult:
    value: str
    score: float


def normalize_text(value: object) -> str:
    text = "" if pd.isna(value) else str(value)
    text = re.sub(r"\s+", "", text)
    return text.strip().casefold()


def similarity(left: object, right: object) -> float:
    left_norm = normalize_text(left)
    right_norm = normalize_text(right)
    if not left_norm or not right_norm:
        return 0.0
    if left_norm == right_norm:
        return 1.0
    return SequenceMatcher(None, left_norm, right_norm).ratio()


def best_match(value: object, candidates: list[str]) -> MatchResult | None:
    observed = normalize_text(value)
    if not observed:
        return None

    best: MatchResult | None = None
    for candidate in candidates:
        score = similarity(observed, candidate)
        if best is None or score > best.score:
            best = MatchResult(value=candidate, score=score)
    return best


def _split_aliases(value: object) -> list[str]:
    if pd.isna(value):
        return []
    return [part.strip() for part in str(value).split("|") if part.strip()]


def load_master_terms(path: Path, canonical_column: str) -> list[str]:
    if not path.exists():
        return []
    frame = pd.read_csv(path).fillna("")
    if canonical_column not in frame.columns:
        return []

    terms: list[str] = []
    for _, row in frame.iterrows():
        canonical = str(row[canonical_column]).strip()
        if canonical:
            terms.append(canonical)
        for alias in _split_aliases(row.get("aliases", "")):
            terms.append(alias)
    return sorted({term for term in terms if term})


def suggest_value(value: object, candidates: list[str], threshold: float) -> tuple[str, float, str]:
    match = best_match(value, candidates)
    if match is None:
        return "", 0.0, "no_observed_value"
    if match.score >= threshold:
        status = "exact" if match.score == 1.0 else "suggested"
    else:
        status = "no_confident_match"
    return match.value, round(match.score, 4), status
