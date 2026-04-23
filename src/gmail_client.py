"""
Gmail OAuth client — loads credentials and exposes search_threads.

Authentication strategy (first of these that works):

  1. Streamlit secret `GMAIL_TOKEN_JSON` — the full token.json contents
     as a blob. Works on Streamlit Cloud where we can't mount files.
     Stashed via st.secrets / os.environ['GMAIL_TOKEN_JSON'].

  2. File path from env var GMAIL_CREDENTIALS_PATH (default:
     '../reputation-audit-tool/credentials/token.json') — local dev.

Refresh is automatic; if the access token is expired google-auth
renews with the refresh_token before the next API call.

Exposes:
  get_gmail_service()      → a googleapiclient Resource (or None if
                              no creds / refresh failed).
  search_threads(query, page_size, page_token) → same shape as the
      MCP tool output {'threads': [...], 'nextPageToken': ...}.
  Each thread has messages with {date, id, sender, snippet, subject,
  toRecipients} — matches what src/gmail_sync.py expects.
"""
from __future__ import annotations

import base64
import json
import logging
import os
import re
from email.utils import parseaddr
from typing import Optional


logger = logging.getLogger(__name__)

_SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/gmail.compose",
]


def _load_credentials():
    """Return google.oauth2.credentials.Credentials or None."""
    try:
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
    except ImportError as e:
        logger.warning(f"google-auth not installed: {e}")
        return None

    info = None

    # 1) Streamlit secret — single JSON blob (best for Streamlit Cloud).
    #    You'd stash this via the Streamlit Secrets UI.
    raw = os.environ.get("GMAIL_TOKEN_JSON")
    if not raw:
        try:
            import streamlit as st
            raw = st.secrets.get("GMAIL_TOKEN_JSON", "")  # type: ignore
        except Exception:
            raw = ""
    if raw:
        try:
            info = json.loads(raw)
        except Exception as e:
            logger.warning(f"GMAIL_TOKEN_JSON present but not valid JSON: {e}")
            info = None

    # 2) File path fallback (local dev).
    if info is None:
        path = os.environ.get(
            "GMAIL_CREDENTIALS_PATH",
            "../reputation-audit-tool/credentials/token.json",
        )
        # Resolve relative to the email-scraper repo root so both worktree
        # and main checkout see the sister repo the same way.
        if not os.path.isabs(path):
            here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            path = os.path.abspath(os.path.join(here, path))
        try:
            with open(path) as f:
                info = json.load(f)
        except FileNotFoundError:
            logger.info(f"Gmail token file not found at {path}")
            return None
        except Exception as e:
            logger.warning(f"Failed to load token.json: {e}")
            return None

    try:
        creds = Credentials.from_authorized_user_info(info, _SCOPES)
    except Exception as e:
        logger.warning(f"Credentials.from_authorized_user_info failed: {e}")
        return None

    # Refresh if expired
    if not creds.valid:
        if creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                logger.warning(f"Gmail token refresh failed: {e}")
                return None
        else:
            logger.warning("Gmail creds invalid and no refresh_token")
            return None
    return creds


def get_gmail_service():
    """Return a Gmail API service, or None if auth isn't set up."""
    creds = _load_credentials()
    if not creds:
        return None
    try:
        from googleapiclient.discovery import build
        return build("gmail", "v1", credentials=creds, cache_discovery=False)
    except Exception as e:
        logger.warning(f"build('gmail', 'v1') failed: {e}")
        return None


def is_available() -> bool:
    return get_gmail_service() is not None


# ── search_threads — MCP-compatible shape ──

_SUBJECT_RE = re.compile(r"^Subject: ?(.*)$", re.I | re.M)


def _header(msg_payload: dict, name: str) -> str:
    for h in (msg_payload.get("headers") or []):
        if h.get("name", "").lower() == name.lower():
            return h.get("value", "")
    return ""


def search_threads(query: str, pageSize: int = 50,
                    pageToken: Optional[str] = None,
                    includeTrash: bool = False) -> dict:
    """
    Same output shape as the MCP search_threads tool. Each thread has:
      id, messages[]
    Each message has:
      id, date (ISO), sender, subject, snippet, toRecipients[]

    This is what src/gmail_sync.py expects — drop-in replacement for
    the MCP callback.
    """
    service = get_gmail_service()
    if not service:
        raise RuntimeError(
            "Gmail not configured. Set GMAIL_TOKEN_JSON (Streamlit secret) "
            "or GMAIL_CREDENTIALS_PATH to a valid token.json."
        )

    # Step 1: list thread IDs matching the query
    params = {
        "userId": "me",
        "q": query,
        "maxResults": min(int(pageSize), 100),
    }
    if pageToken:
        params["pageToken"] = pageToken
    if includeTrash:
        params["includeSpamTrash"] = True

    resp = service.users().threads().list(**params).execute()
    thread_ids = [t["id"] for t in (resp.get("threads") or [])]
    next_token = resp.get("nextPageToken")

    out_threads = []
    for tid in thread_ids:
        t = service.users().threads().get(
            userId="me", id=tid, format="metadata",
            metadataHeaders=["Subject", "From", "To", "Date"],
        ).execute()
        messages = []
        for m in (t.get("messages") or []):
            p = m.get("payload", {}) or {}
            subject = _header(p, "Subject")
            from_hdr = _header(p, "From")
            to_hdr = _header(p, "To")
            _, sender = parseaddr(from_hdr)
            to_addrs = [
                parseaddr(x)[1]
                for x in re.split(r"[,;]", to_hdr)
                if parseaddr(x)[1]
            ]
            # Gmail internal timestamp (ms since epoch)
            internal_ms = int(m.get("internalDate", "0"))
            from datetime import datetime, timezone
            dt = datetime.fromtimestamp(internal_ms / 1000, tz=timezone.utc)
            messages.append({
                "id": m.get("id"),
                "date": dt.isoformat().replace("+00:00", "Z"),
                "sender": sender or from_hdr,
                "subject": subject,
                "snippet": m.get("snippet", ""),
                "toRecipients": to_addrs,
            })
        out_threads.append({"id": tid, "messages": messages})

    return {"threads": out_threads, "nextPageToken": next_token}
