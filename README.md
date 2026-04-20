# automation-engine
Auto File Transfer to server
# FAS — File Automation System

Headless background service that watches **multiple folders**, each with its own upload settings, retry policy, and metadata. All results are stored in **one SQLite database** — one table per folder — tracking every file, whether sent or failed.

---

## Key Design

| Concern | Approach |
|---|---|
| Multi-folder | Each entry in `"folders"` is an independent profile |
| Database | Single `fas.db`; every folder gets its own table |
| File tracking | **Every** file is recorded — `sent`, `pending`, `retrying`, or `failed` |
| Retry | Per-folder retry loop with its own interval and max-attempt settings |
| Config | External `config.json`, hot-reloaded every 30s without restart |
| Packaging | PyInstaller single EXE; `config.json` ships next to EXE |

---

## Project Structure

```
fas/
├── main.py               # CLI: run / retry / status / validate-config
├── config.json           # Multi-folder config (never embedded into EXE)
├── requirements.txt
├── fas.spec              # PyInstaller spec
└── fas/
    ├── config_loader.py  # FolderProfile + Config + hot-reload
    ├── watcher.py        # One watchdog observer per folder
    ├── processor.py      # Pipeline: validate → DB insert → upload → DB update
    ├── uploader.py       # FTP / REST API adapters
    ├── retry_engine.py   # One retry loop per folder
    ├── db.py             # SQLite: one table per folder, full file history
    └── logger.py         # Structured JSON logs
```

---

## config.json

```json
{
  "database": { "path": "fas.db" },
  "logging":  { "level": "INFO", "file": "fas.log" },

  "folders": [
    {
      "name": "invoices",
      "watch": {
        "path": "C:\\Uploads\\Invoices",
        "depth": 1,
        "extensions": [".pdf", ".xml"]
      },
      "upload": {
        "type": "ftp",
        "ftp": { "host": "ftp.example.com", "port": 21, "user": "u", "password": "p" }
      },
      "metadata": { "usercode": "INV001", "folder_id": "10" },
      "retry":    { "interval_seconds": 60, "max_attempts": 5 }
    },
    {
      "name": "reports",
      "watch": {
        "path": "C:\\Uploads\\Reports",
        "depth": 2,
        "extensions": [".pdf", ".docx"]
      },
      "upload": {
        "type": "api",
        "endpoint": "https://api.example.com/upload"
      },
      "metadata": { "usercode": "RPT002" },
      "retry":    { "interval_seconds": 120, "max_attempts": 3 }
    }
  ]
}
```

Add as many folders as needed. Each folder entry is fully independent.

---

## SQLite Schema (per folder)

One table is created per folder, named after the `"name"` field (sanitised for SQL).

```sql
CREATE TABLE invoices (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    file_path     TEXT    NOT NULL,         -- full path on disk
    file_name     TEXT    NOT NULL,         -- filename only
    file_size     INTEGER,                  -- bytes
    metadata_json TEXT    NOT NULL,         -- snapshot of metadata at time of send
    attempt_count INTEGER NOT NULL DEFAULT 0,
    first_seen    TEXT    NOT NULL,         -- ISO-8601 UTC
    last_attempt  TEXT,
    next_retry    TEXT,
    status        TEXT    NOT NULL,         -- sent | pending | retrying | failed
    error         TEXT                      -- last error message
);
```

**Status lifecycle:**

```
[file detected]
      │
      ▼
   pending  ──upload OK──▶  sent        ✅ terminal
      │
      │ upload fails
      ▼
   pending  ──retry──▶  retrying  ──OK──▶  sent
                            │
                            │ fails again (attempt_count < max)
                            ▼
                         pending (scheduled for next_retry)
                            │
                            │ attempt_count >= max_attempts
                            ▼
                          failed  ❌ terminal (manual intervention needed)
```

---

## CLI Usage

```bash
# Start everything (watch all folders + retry daemons)
python main.py run

# Override config location
python main.py --config D:\conf\fas.json run

# Manual retry pass across all folders
python main.py retry

# Per-folder stats (JSON output)
python main.py status

# Validate config — exit 0 = ok, 1 = errors
python main.py validate-config
```

### Example `status` output

```json
{
  "config_path": "C:\\fas\\config.json",
  "database": "fas.db",
  "folders": [
    {
      "name": "invoices",
      "table": "invoices",
      "watch_path": "C:\\Uploads\\Invoices",
      "upload_type": "ftp",
      "stats": { "sent": 142, "pending": 2, "failed": 1 }
    },
    {
      "name": "reports",
      "table": "reports",
      "watch_path": "C:\\Uploads\\Reports",
      "upload_type": "api",
      "stats": { "sent": 37 }
    }
  ]
}
```

---

## Sample Log Lines

```json
{"timestamp":"2025-09-01T14:20:00","level":"INFO","event":"startup","status":"ok","detail":"3 folder(s) configured"}
{"timestamp":"2025-09-01T14:20:01","level":"INFO","event":"watch","status":"started","folder":"invoices","file":"C:\\Uploads\\Invoices"}
{"timestamp":"2025-09-01T14:20:01","level":"INFO","event":"watch","status":"started","folder":"reports","file":"C:\\Uploads\\Reports"}
{"timestamp":"2025-09-01T14:22:05","level":"INFO","event":"watch","status":"enqueued","folder":"invoices","file":"C:\\Uploads\\Invoices\\inv_0042.pdf"}
{"timestamp":"2025-09-01T14:22:06","level":"INFO","event":"upload","status":"sent","folder":"invoices","file":"C:\\Uploads\\Invoices\\inv_0042.pdf","latency_ms":823.4}
{"timestamp":"2025-09-01T14:23:10","level":"ERROR","event":"upload","status":"error","folder":"reports","file":"C:\\Uploads\\Reports\\q3.pdf","attempt":1,"error":"ConnectionRefusedError"}
{"timestamp":"2025-09-01T14:25:10","level":"INFO","event":"retry","status":"attempt","folder":"reports","file":"C:\\Uploads\\Reports\\q3.pdf","attempt":2}
{"timestamp":"2025-09-01T14:25:12","level":"INFO","event":"retry","status":"sent","folder":"reports","file":"C:\\Uploads\\Reports\\q3.pdf","attempt":2,"latency_ms":1203.1}
```

---

## Installation

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

---

## PyInstaller EXE (Windows)

```bash
pip install pyinstaller watchdog requests
pyinstaller fas.spec
```

Distribute these two files together:
```
dist\
  fas.exe
  config.json    ← edit freely; never rebuild
```

Run as Windows service via [NSSM](https://nssm.cc/):
```cmd
nssm install FAS "C:\fas\dist\fas.exe" run
nssm set FAS AppDirectory "C:\fas\dist"
nssm start FAS
```

---

## Environment Variables

| Variable | Purpose |
|---|---|
| `FAS_CONFIG` | Override config.json path |
