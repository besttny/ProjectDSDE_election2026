import json
import py_compile
import re
import sys
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_DIR = ROOT / "notebooks"


def _notebook_code_cells(path: Path) -> list[str]:
    notebook = json.loads(path.read_text(encoding="utf-8"))
    return [
        "".join(cell.get("source", []))
        for cell in notebook.get("cells", [])
        if cell.get("cell_type") == "code"
    ]


def test_python_entrypoints_compile():
    for path in [ROOT / "run_batch.py", NOTEBOOK_DIR / "dashboard.py"]:
        py_compile.compile(str(path), doraise=True)


def test_all_notebook_code_cells_compile():
    for path in sorted(NOTEBOOK_DIR.glob("*.ipynb")):
        for index, source in enumerate(_notebook_code_cells(path), start=1):
            compile(source, f"{path}:cell{index}", "exec")


def test_ocr_notebook_full_run_is_guarded_by_default():
    source = "\n".join(_notebook_code_cells(NOTEBOOK_DIR / "02_ocr_extraction.ipynb"))

    assert "run_batch(force=True)" not in source
    assert re.search(r"RUN_FULL_OCR\s*=\s*False", source)
    assert re.search(r"RUN_OCR_SMOKE_TEST\s*=\s*True", source)
    assert re.search(r"OCR_FORCE\s*=\s*False", source)


def test_ocr_api_key_is_checked_only_when_requesting_ocr():
    source = "\n".join(_notebook_code_cells(NOTEBOOK_DIR / "02_ocr_extraction.ipynb"))

    assert "raise RuntimeError(\n        \"Set TYPHOON_API_KEY in your environment first." not in source
    assert 'api_key = os.getenv("TYPHOON_API_KEY") or TYPHOON_API_KEY' in source
    assert 'headers = {"Authorization": f"Bearer {api_key}"}' in source


def test_validation_flag_splitter_supports_legacy_and_current_separators():
    sys.path.insert(0, str(ROOT))
    import run_batch

    flags = run_batch.split_validation_flags(pd.Series(["alpha;bravo|charlie:1", "", None]))

    assert flags.tolist() == ["alpha", "bravo", "charlie:1"]
