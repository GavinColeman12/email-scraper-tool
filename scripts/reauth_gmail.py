#!/usr/bin/env python3
"""Re-authenticate Gmail API access.

When the OAuth token expires or is revoked (typically after 90 days of
inactivity, or if you revoke it manually in your Google account security
settings), the auto-send script fails with 'invalid_grant'. Run this once
to get a fresh token.

Process:
  1. Opens your default browser to Google's OAuth consent screen
  2. You sign in with the Google account you want to send from
  3. Click "Allow" to grant the Gmail scopes
  4. New token saved to ../reputation-audit-tool/credentials/token.json
  5. Auto-send script works again

Usage:
    cd /Users/gavincoleman/Downloads/email-scraper
    python3 scripts/reauth_gmail.py
"""

from __future__ import annotations
import json
import sys
from pathlib import Path

SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/gmail.compose",
]

# Resolve paths to the sister repo where credentials live
REPO_ROOT = Path(__file__).resolve().parent.parent
CREDS_DIR = REPO_ROOT.parent / "reputation-audit-tool" / "credentials"
CLIENT_SECRET = CREDS_DIR / "client_secret.json"
TOKEN_FILE = CREDS_DIR / "token.json"


def main() -> int:
    # Resolve credential paths — try primary, fall back to V2
    client_secret = CLIENT_SECRET
    token_file = TOKEN_FILE
    if not client_secret.exists():
        alt = REPO_ROOT.parent / "reputation-audit-tool-V2" / "credentials" / "client_secret.json"
        if alt.exists():
            print(f"Using fallback credentials at {alt.parent}")
            client_secret = alt
            token_file = alt.parent / "token.json"
        else:
            print(f"❌ Can't find client_secret.json at {client_secret}")
            print(f"   Also checked: {alt}")
            print(f"   Make sure your OAuth client config exists in the credentials/ dir.")
            return 1

    try:
        from google_auth_oauthlib.flow import InstalledAppFlow
    except ImportError:
        print("❌ google-auth-oauthlib not installed. Install with:")
        print("   pip install google-auth-oauthlib")
        return 1

    print(f"Using client config: {client_secret}")
    print(f"Will save token to: {token_file}")
    print()
    print("Opening browser for Google sign-in...")
    print("Sign in with the Google account you want to SEND emails as.")
    print()

    flow = InstalledAppFlow.from_client_secrets_file(str(client_secret), SCOPES)
    creds = flow.run_local_server(port=0, prompt="consent", access_type="offline")

    # Save token
    with token_file.open("w") as f:
        f.write(creds.to_json())

    print(f"\n✅ Token saved to {token_file}")
    print(f"   Refresh token included: {'yes' if creds.refresh_token else 'NO — re-run with access_type=offline'}")
    print()

    # Quick verification
    print("Verifying with Gmail API...")
    try:
        from googleapiclient.discovery import build
        service = build("gmail", "v1", credentials=creds, cache_discovery=False)
        profile = service.users().getProfile(userId="me").execute()
        print(f"✅ Authenticated as: {profile.get('emailAddress')}")
        print(f"   Total messages: {profile.get('messagesTotal')}")
        print(f"   Total threads: {profile.get('threadsTotal')}")
    except Exception as e:
        print(f"⚠️  Auth saved but Gmail API test failed: {e}")
        return 1

    print()
    print("🎉 Done. Try auto_send_outreach.py now.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
