"""
PDF scraping — extract text + emails from PDFs linked on a business
website. Free (pypdf + requests).

Lawyers, clinics, and consulting firms publish brochures, CVs, and
engagement letters as PDFs. Staff emails land in headers, footers, and
signature blocks. This module fetches those PDFs and runs the same
email regex as the HTML pipeline.

Public API:
  harvest_pdf_emails(pdf_urls, *, domain, max_pdfs=5, timeout_s=10)
    → {"emails": set, "source_urls": set}
"""
from __future__ import annotations

import logging
import re
from typing import Iterable

import requests


logger = logging.getLogger(__name__)

_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")

_MAX_PDF_BYTES = 6 * 1024 * 1024   # 6 MB hard cap per PDF
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
}


def _extract_text(pdf_bytes: bytes) -> str:
    """Try pypdf first, then pdfplumber as a fallback. Returns '' on failure."""
    try:
        import pypdf  # type: ignore
        from io import BytesIO
        reader = pypdf.PdfReader(BytesIO(pdf_bytes))
        parts = []
        # Cap at 50 pages — engagement letters are short; CVs up to 20
        for page in reader.pages[:50]:
            try:
                parts.append(page.extract_text() or "")
            except Exception:
                continue
        return "\n".join(parts)
    except Exception:
        pass
    try:
        import pdfplumber  # type: ignore
        from io import BytesIO
        text = []
        with pdfplumber.open(BytesIO(pdf_bytes)) as doc:
            for page in doc.pages[:50]:
                text.append(page.extract_text() or "")
        return "\n".join(text)
    except Exception:
        return ""


def harvest_pdf_emails(
    pdf_urls: Iterable[str],
    *,
    domain: str = "",
    max_pdfs: int = 5,
    timeout_s: int = 10,
) -> dict:
    """
    Download up to `max_pdfs` PDFs and return the @domain emails found
    in them. Skips PDFs over 6 MB.

    Returns {"emails": set[str], "source_urls": set[str]}.
    """
    out_emails: set[str] = set()
    out_sources: set[str] = set()
    dom_suffix = f"@{domain.lower()}" if domain else ""
    fetched = 0

    for url in pdf_urls:
        if fetched >= max_pdfs:
            break
        if not url or not url.lower().endswith(".pdf"):
            continue
        try:
            r = requests.get(url, timeout=timeout_s, headers=_HEADERS,
                             stream=True, allow_redirects=True)
            if r.status_code != 200:
                continue
            # Peek content-type + length
            ct = r.headers.get("content-type", "").lower()
            if "pdf" not in ct and "application/octet-stream" not in ct:
                continue
            cl = int(r.headers.get("content-length") or 0)
            if cl and cl > _MAX_PDF_BYTES:
                continue
            data = r.content[:_MAX_PDF_BYTES]
        except Exception as e:
            logger.debug(f"pdf fetch {url}: {e}")
            continue
        fetched += 1
        text = _extract_text(data)
        if not text:
            continue
        for email in _EMAIL_RE.findall(text):
            e = email.lower()
            if dom_suffix and not e.endswith(dom_suffix):
                continue
            out_emails.add(e)
            out_sources.add(url)

    return {"emails": out_emails, "source_urls": out_sources}
