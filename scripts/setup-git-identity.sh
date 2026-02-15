#!/bin/bash
# Interactive Git identity setup
# Run: ./scripts/setup-git-identity.sh

echo "=== Git Identity Setup ==="
echo "Use the same email as your GitHub account."
echo ""

read -p "Your name (e.g. John Doe): " NAME
read -p "Your email (e.g. john@example.com): " EMAIL

if [ -z "$NAME" ] || [ -z "$EMAIL" ]; then
  echo "Name and email are required. Aborted."
  exit 1
fi

git config --global user.name "$NAME"
git config --global user.email "$EMAIL"
git config --global init.defaultBranch main

echo ""
echo "✓ Git identity set:"
echo "  user.name  = $NAME"
echo "  user.email = $EMAIL"
echo ""
echo "Run ./scripts/verify-setup.sh to check everything."
