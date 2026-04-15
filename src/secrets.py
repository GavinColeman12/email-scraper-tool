"""Unified secrets loader: .env file OR Streamlit Cloud secrets."""
import os
from pathlib import Path
from dotenv import load_dotenv


def _load_env_files():
    here = Path(__file__).resolve()
    for parent in [here.parent, *here.parents]:
        env_path = parent / ".env"
        if env_path.exists():
            load_dotenv(env_path, override=True)
    load_dotenv(override=False)


_load_env_files()


def get_secret(key: str) -> str:
    """Get a secret from Streamlit Cloud secrets or .env fallback."""
    try:
        import streamlit as st
        if hasattr(st, "secrets"):
            try:
                val = st.secrets.get(key) if hasattr(st.secrets, "get") else None
                if val:
                    return str(val).strip()
            except Exception:
                pass
    except Exception:
        pass

    value = os.getenv(key, "").strip()
    if not value:
        _load_env_files()
        value = os.getenv(key, "").strip()
    return value
