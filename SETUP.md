# SAS Awards – Local Setup & Git Verification

## Current Status ✓

| Item | Status | Notes |
|------|--------|-------|
| Credential helper | ✓ | `osxkeychain` (macOS) |
| Git user.name | ✗ | **Not set** |
| Git user.email | ✗ | **Not set** |
| SSH keys | ✗ | None found in `~/.ssh` |
| GitHub credentials | ✗ | None in Keychain |
| GitHub CLI | ✗ | Not installed or not authenticated |

---

## Step 1: Set Git Identity (required for commits)

Run these with your real name and email (use the same email as your GitHub account):

```bash
git config --global user.name "Your Name"
git config --global user.email "your.email@example.com"
```

Optional but useful:

```bash
git config --global init.defaultBranch main
```

---

## Step 2: Authenticate with GitHub

### Option A: SSH (recommended for long-term use)

1. **Generate an Ed25519 key:**
   ```bash
   ssh-keygen -t ed25519 -C "your.email@example.com" -f ~/.ssh/id_ed25519 -N ""
   ```
   (Remove `-N ""` if you want a passphrase prompt.)

2. **Start the ssh-agent and add the key:**
   ```bash
   eval "$(ssh-agent -s)"
   ssh-add ~/.ssh/id_ed25519
   ```

3. **Copy your public key to the clipboard:**
   ```bash
   pbcopy < ~/.ssh/id_ed25519.pub
   ```

4. **Add it to GitHub:** [GitHub → Settings → SSH and GPG keys](https://github.com/settings/keys) → New SSH key → paste → Save.

5. **Use SSH for the remote:**
   ```bash
   cd /Users/jeppe/sas-awards
   git remote set-url origin git@github.com:jsmooother/sas_awards.git
   ```

### Option B: HTTPS with Personal Access Token (PAT)

1. **Create a token:** [GitHub → Settings → Developer settings → Personal access tokens](https://github.com/settings/tokens)
   - Fine-grained: give repo access and Contents read/write
   - Classic: use `repo` scope

2. **On first push or fetch**, Git will ask for credentials:
   - Username: your GitHub username
   - Password: paste the **PAT**, not your GitHub password

3. **macOS will store it** via `osxkeychain`.

---

## Step 3: Verify Setup

Run:

```bash
./scripts/verify-setup.sh
```

Or manually:

```bash
# Check Git config
git config --global user.name
git config --global user.email

# Test GitHub (SSH)
ssh -T git@github.com

# Or test HTTPS fetch
cd /Users/jeppe/sas-awards && git fetch origin
```

---

## Step 4: Initialize Project (if empty repo)

If the remote has no commits yet:

```bash
cd /Users/jeppe/sas-awards
# Add your files, then:
git add .
git commit -m "Initial commit"
git push -u origin main
```

If the remote has commits:

```bash
cd /Users/jeppe/sas-awards
git pull origin main
```

---

## Quick Reference

| Task | Command |
|------|---------|
| Check identity | `git config user.name` / `git config user.email` |
| Test SSH | `ssh -T git@github.com` |
| Fetch from GitHub | `git fetch origin` |
| Switch to SSH remote | `git remote set-url origin git@github.com:jsmooother/sas_awards.git` |
