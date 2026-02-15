#!/usr/bin/env python3
"""
Verify Telegram bot token. Run from project root:
  TELEGRAM_BOT_TOKEN=your_token python scripts/verify_telegram.py
Or with .env: python scripts/verify_telegram.py (loads .env if python-dotenv installed)
"""
import os
import sys

# Try to load .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
if not TOKEN:
    print("ERROR: TELEGRAM_BOT_TOKEN not set.")
    print()
    print("1. Create a bot: message @BotFather on Telegram → /newbot")
    print("2. Copy the token (e.g. 123456789:ABC...)")
    print("3. Either:")
    print("   export TELEGRAM_BOT_TOKEN='your_token'")
    print("   or create .env with: TELEGRAM_BOT_TOKEN=your_token")
    sys.exit(1)

import urllib.request
import json

url = f"https://api.telegram.org/bot{TOKEN}/getMe"
try:
    with urllib.request.urlopen(url, timeout=10) as r:
        data = json.loads(r.read().decode())
except urllib.error.HTTPError as e:
    body = e.read().decode() if e.fp else ""
    print(f"HTTP error {e.code}: {body[:200]}")
    if "401" in str(e.code):
        print("\n→ Token is invalid. Check it from @BotFather.")
    sys.exit(1)
except Exception as e:
    print(f"Network error: {e}")
    sys.exit(1)

if not data.get("ok"):
    print("API error:", data)
    sys.exit(1)

bot = data.get("result", {})
print("✓ Telegram bot verified")
print(f"  Username: @{bot.get('username', '?')}")
print(f"  Name: {bot.get('first_name', '?')}")
print()
print("To run the bot:")
print("  cd /Users/jeppe/sas-awards")
print("  TELEGRAM_BOT_TOKEN=your_token python weekend_bot.py")
print()
print("Or add TELEGRAM_BOT_TOKEN to .env and run: python weekend_bot.py")
