#!/bin/bash
# SAS Awards - Git & GitHub setup verification
# Run from project root: ./scripts/verify-setup.sh

set -e
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=== Git & GitHub Setup Verification ==="
echo ""

# 1. Git identity
echo "1. Git identity"
if git config --global user.name >/dev/null 2>&1; then
  echo -e "   ${GREEN}✓${NC} user.name: $(git config --global user.name)"
else
  echo -e "   ${RED}✗${NC} user.name not set. Run: git config --global user.name \"Your Name\""
fi
if git config --global user.email >/dev/null 2>&1; then
  echo -e "   ${GREEN}✓${NC} user.email: $(git config --global user.email)"
else
  echo -e "   ${RED}✗${NC} user.email not set. Run: git config --global user.email \"your@email.com\""
fi
echo ""

# 2. Credential helper
echo "2. Credential helper"
HELPER=$(git config --get credential.helper 2>/dev/null || echo "none")
if [ -n "$HELPER" ] && [ "$HELPER" != "none" ]; then
  echo -e "   ${GREEN}✓${NC} $HELPER"
else
  echo -e "   ${YELLOW}!${NC} No credential helper (macOS usually has osxkeychain)"
fi
echo ""

# 3. SSH key
echo "3. SSH key (for git@github.com)"
if [ -f ~/.ssh/id_ed25519.pub ] || [ -f ~/.ssh/id_rsa.pub ]; then
  KEY_FILE=$(ls ~/.ssh/id_ed25519.pub ~/.ssh/id_rsa.pub 2>/dev/null | head -1)
  echo -e "   ${GREEN}✓${NC} Found: $KEY_FILE"
  echo "   Add to GitHub: https://github.com/settings/keys"
else
  echo -e "   ${YELLOW}!${NC} No SSH key found. Generate with: ssh-keygen -t ed25519 -C \"your@email.com\""
fi
echo ""

# 4. Remote
echo "4. Git remote"
cd "$(dirname "$0")/.."
REMOTE=$(git remote get-url origin 2>/dev/null || echo "none")
echo "   origin: $REMOTE"
if [[ "$REMOTE" == *"git@github.com"* ]]; then
  echo -e "   ${GREEN}✓${NC} Using SSH"
elif [[ "$REMOTE" == *"github.com"* ]]; then
  echo -e "   ${YELLOW}!${NC} Using HTTPS (will need PAT for private repos)"
fi
echo ""

# 5. Test connection (optional)
echo "5. Connection test"
if [[ "$REMOTE" == *"git@github.com"* ]]; then
  echo "   Testing: ssh -T git@github.com"
  if ssh -T git@github.com 2>&1 | grep -q "successfully authenticated"; then
    echo -e "   ${GREEN}✓${NC} GitHub SSH connection OK"
  else
    echo -e "   ${YELLOW}!${NC} Run manually: ssh -T git@github.com"
  fi
else
  echo "   Skip (using HTTPS - test with: git fetch origin)"
fi
echo ""
echo "Done. See SETUP.md for full setup guide."
