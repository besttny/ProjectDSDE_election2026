from __future__ import annotations

from pathlib import Path


def render_pdf_pages(
    pdf_path: Path,
    output_dir: Path,
    *,
    dpi: int = 300,
    image_format: str = "png",
    limit_pages: int | None = None,
) -> list[Path]:
    """Render PDF pages to images and return the generated image paths."""

    try:
        import fitz
    except ImportError as exc:  # pragma: no cover - exercised in environment setup
        raise RuntimeError("PyMuPDF is required for PDF rendering. Install pymupdf.") from exc

    output_dir.mkdir(parents=True, exist_ok=True)
    generated: list[Path] = []
    zoom = dpi / 72
    matrix = fitz.Matrix(zoom, zoom)

    with fitz.open(pdf_path) as document:
        page_count = len(document)
        if limit_pages is not None:
            page_count = min(page_count, limit_pages)
        for page_index in range(page_count):
            page = document.load_page(page_index)
            pixmap = page.get_pixmap(matrix=matrix, alpha=False)
            image_path = output_dir / f"{pdf_path.stem}_page_{page_index + 1:04d}.{image_format}"
            pixmap.save(image_path)
            generated.append(image_path)
    return generated

