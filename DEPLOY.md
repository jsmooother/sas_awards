# Deploy on backend (Mac mini)

Manual pull and restart. Run from the repo directory (e.g. `~/sas_awards`).

```bash
git pull origin main
source venv/bin/activate
# Restart the app (e.g. if run with python app.py, stop it and run again)
python app.py
```

If you run the **Partner Awards worker** (Flying Blue scans), restart that process too after pulling.

DB: No migration step needed. The app runs `init_db` on first use and creates/updates partner tables automatically.
