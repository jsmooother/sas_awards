# GitHub Setup Guide

## Initial setup (first time)

### 1. Create a new repository on GitHub

1. Go to [github.com/new](https://github.com/new)
2. Set **Repository name:** `sas_awards` (or your preferred name)
3. Choose **Public** or **Private**
4. **Do not** add README, .gitignore, or license – we already have these locally
5. Click **Create repository**

### 2. Initialize git and add remote

From your project directory:

```bash
cd /path/to/sas_awards   # or ~/sas_awards

# Initialize git
git init

# Add the remote (replace YOUR_USERNAME and REPO_NAME with your values)
git remote add origin https://github.com/YOUR_USERNAME/sas_awards.git

# Or with SSH:
# git remote add origin git@github.com:YOUR_USERNAME/sas_awards.git
```

### 3. Stage and commit

```bash
git add .
git status   # Review what will be committed (venv, .env, *.sqlite should be ignored)
git commit -m "Initial commit: SAS awards tracker and Telegram bot"
```

### 4. Push to GitHub

```bash
git branch -M main
git push -u origin main
```

---

## Before pushing – security check

Make sure **no secrets** are committed:

- [ ] `.env` is in `.gitignore` (it is)
- [ ] Bot token is in environment or `.env`, **not** hardcoded
- [ ] No `*.sqlite` or database files committed

Run:

```bash
git status
# Ensure .env, venv/, *.sqlite, *.log are NOT staged
```

---

## SSH vs HTTPS

| Method | URL format | Notes |
|--------|------------|-------|
| HTTPS | `https://github.com/USER/REPO.git` | Prompts for username/password or token |
| SSH | `git@github.com:USER/REPO.git` | Requires SSH key; no password for push |

### Set up SSH (recommended)

1. Generate key: `ssh-keygen -t ed25519 -C "your_email@example.com"`
2. Add to ssh-agent: `eval "$(ssh-agent -s)"` then `ssh-add ~/.ssh/id_ed25519`
3. Copy public key: `pbcopy < ~/.ssh/id_ed25519.pub` (macOS)
4. GitHub → Settings → SSH and GPG keys → New SSH key → Paste

Then use `git remote add origin git@github.com:USER/sas_awards.git`.

---

## Common commands

```bash
git status              # See changes
git add .               # Stage all (respects .gitignore)
git commit -m "msg"     # Commit
git push                # Push to GitHub
git pull                # Pull latest
```
