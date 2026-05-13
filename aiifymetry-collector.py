import os
import re
import sys
import json
import glob
import time
import copy
import sqlite3
import subprocess
from datetime import datetime
from threading import Thread

# --- BOOTSTRAP DEPENDENCIES ---
def _bootstrap():
    required = ["requests", "psutil"]
    pip_ok = True
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "--version"],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError:
        pip_ok = False
    if not pip_ok:
        try:
            subprocess.check_call([sys.executable, "-m", "ensurepip", "--user"],
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            pip_ok = True
        except: pass
    for pkg in required:
        try: __import__(pkg)
        except ImportError:
            if pip_ok:
                try: subprocess.check_call([sys.executable, "-m", "pip", "install", pkg, "--quiet"])
                except: pass

_bootstrap()

try: import requests
except ImportError: print("CRITICAL: 'requests' missing."); sys.exit(1)

try: import psutil; HAS_PSUTIL = True
except ImportError: HAS_PSUTIL = False

# ── Configuration ───────────────────────────────────────────────────────────────

INGESTOR_URL  = "https://aiifymetry-34805915210.us-central1.run.app"
GATEWAY_TOKEN = (os.getenv("CLAW_GATEWAY_TOKEN") or "").strip()
INSTANCE_ID   = os.getenv("CLAW_INSTANCE_ID", "new-instance")
CUSTOMER_ID   = os.getenv("CLAW_CUSTOMER_ID", "default")

OC = os.path.expanduser("~/.openclaw")

OPENCLAW_SESSIONS = os.path.join(OC, "agents/main/sessions/*.jsonl")
CLAUDE_LOGS       = os.path.expanduser("~/.claude/projects/**/*.jsonl")

# Skill roots — (dir, source_label)
SKILL_ROOTS = [
    (os.path.expanduser("~/.hermes/skills/"),             "hermes"),
    (os.path.join(OC, "skills/"),                         "openclaw"),
    (os.path.join(OC, "workspace/skills/"),               "workspace"),
    (os.path.expanduser("~/.npm-global/lib/node_modules/openclaw/skills/"), "builtin"),
]

NPM_OC_ROOT = os.path.expanduser("~/.npm-global/lib/node_modules/openclaw")

WORKSPACE_DOC_ROOTS = [os.path.join(OC, "workspace/")]
MEMORY_DOC_ROOTS    = [os.path.join(OC, "workspace/memory/")]
MEMORY_MAX_FILES    = 20
MD_MAX_BYTES        = 8 * 1024

# Only tail canonical session files; skip archived/temp variants
_SESSION_RE = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
    r'(-topic-[0-9a-f-]+)?\.jsonl$',
    re.IGNORECASE,
)

SQLITE_POLL = 30  # seconds between SQLite polls

# ── Core helpers ────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.utcnow().isoformat()

def push_events(events, timeout=10):
    if not events or not GATEWAY_TOKEN: return
    payload = {"instance_id": INSTANCE_ID, "customer_id": CUSTOMER_ID, "events": events}
    try:
        r = requests.post(
            f"{INGESTOR_URL}/ingest", json=payload,
            headers={"X-Gateway-Token": GATEWAY_TOKEN}, timeout=timeout,
        )
        if r.status_code != 200:
            print(f"  ⚠ Ingest {r.status_code}: {r.text[:120]}")
    except Exception as e:
        print(f"  ⚠ push_events: {e}")

# ── Session file filter ─────────────────────────────────────────────────────────

def _is_live_session(path: str) -> bool:
    return bool(_SESSION_RE.match(os.path.basename(path)))

# ── Skill collection ────────────────────────────────────────────────────────────

def _read_skill_desc(skill_dir: str):
    for cand in ("SKILL.md", "README.md", "readme.md"):
        p = os.path.join(skill_dir, cand)
        if os.path.isfile(p):
            try:
                with open(p, 'r', errors='replace') as f:
                    return f.read(MD_MAX_BYTES).strip() or None
            except: pass
    return None

def _collect_skills():
    seen, skills = set(), []

    for root, source in SKILL_ROOTS:
        if not os.path.exists(root): continue
        for item in os.listdir(root):
            if item.startswith('.') or item.startswith('__'): continue
            stem = item
            for ext in ('.skill', '.zip'):
                if item.endswith(ext): stem = item[:-len(ext)]; break
            if stem in seen: continue
            seen.add(stem)
            entry = {"name": stem, "source": source}
            full = os.path.join(root, item)
            if os.path.isdir(full):
                desc = _read_skill_desc(full)
                if desc: entry["description"] = desc
            skills.append(entry)

    # Also scan dist/extensions/*/skills/ inside the NPM openclaw package
    ext_root = os.path.join(NPM_OC_ROOT, "dist/extensions")
    if os.path.isdir(ext_root):
        for ext_dir in os.listdir(ext_root):
            skills_sub = os.path.join(ext_root, ext_dir, "skills")
            if not os.path.isdir(skills_sub): continue
            for item in os.listdir(skills_sub):
                if item.startswith('.') or item.startswith('__'): continue
                stem = item
                for sfx in ('.skill', '.zip'):
                    if item.endswith(sfx): stem = item[:-len(sfx)]; break
                if stem in seen: continue
                seen.add(stem)
                entry = {"name": stem, "source": "builtin"}
                full = os.path.join(skills_sub, item)
                if os.path.isdir(full):
                    desc = _read_skill_desc(full)
                    if desc: entry["description"] = desc
                skills.append(entry)

    return skills

# ── Workspace / memory doc collection ──────────────────────────────────────────

def _read_md(path: str) -> dict:
    try:
        with open(path, 'r', errors='replace') as f:
            text = f.read(MD_MAX_BYTES)
        entry = {"content": text.strip()}
        if len(text) >= MD_MAX_BYTES: entry["truncated"] = True
        return entry
    except: return {}

def _collect_workspace_docs():
    docs, seen = [], set()
    for root_path in WORKSPACE_DOC_ROOTS:
        if not os.path.exists(root_path): continue
        for fname in os.listdir(root_path):
            if not fname.endswith('.md'): continue
            full = os.path.join(root_path, fname)
            if not os.path.isfile(full) or full in seen: continue
            seen.add(full)
            entry = {"name": fname, "path": full, "category": "workspace"}
            entry.update(_read_md(full))
            docs.append(entry)

    mem_files = []
    for root_path in MEMORY_DOC_ROOTS:
        if not os.path.exists(root_path): continue
        for root, dirs, files in os.walk(root_path):
            dirs[:] = [d for d in dirs if not d.startswith('.')]
            for fname in files:
                if not fname.endswith('.md'): continue
                full = os.path.join(root, fname)
                try: mem_files.append((os.path.getmtime(full), full, fname))
                except: pass
    mem_files.sort(reverse=True)
    for _, full, fname in mem_files[:MEMORY_MAX_FILES]:
        if full in seen: continue
        seen.add(full)
        entry = {"name": fname, "path": full, "category": "memory"}
        entry.update(_read_md(full))
        docs.append(entry)

    return docs

# ── Extra metadata sources ──────────────────────────────────────────────────────

def _openclaw_version():
    pkg = os.path.join(NPM_OC_ROOT, "package.json")
    if not os.path.isfile(pkg): return None
    try:
        with open(pkg, 'r') as f: return json.load(f).get("version")
    except: return None

def _sanitize_models(cfg: dict) -> dict:
    safe = copy.deepcopy(cfg)
    for v in safe.get("providers", {}).values():
        for k in ("api_key", "apiKey", "key", "secret"):
            v.pop(k, None)
    return safe

def _models_config():
    p = os.path.join(OC, "agents/main/agent/models.json")
    if not os.path.isfile(p): return None
    try:
        with open(p, 'r') as f: return _sanitize_models(json.load(f))
    except: return None

def _cron_jobs():
    p = os.path.join(OC, "cron/jobs.json")
    if not os.path.isfile(p): return []
    try:
        with open(p, 'r') as f:
            data = json.load(f)
            return data if isinstance(data, list) else data.get("jobs", [])
    except: return []

def _completions_count() -> int:
    d = os.path.join(OC, "completions")
    if not os.path.isdir(d): return 0
    try: return sum(1 for _ in os.scandir(d))
    except: return 0

def _sessions_context() -> dict:
    p = os.path.join(OC, "sessions/sessions.json")
    if not os.path.isfile(p): return {}
    try:
        with open(p, 'r') as f: return json.load(f)
    except: return {}

def _memory_rag_stats() -> dict:
    db_path = os.path.join(OC, "memory/main.sqlite")
    if not os.path.isfile(db_path): return {}
    try:
        con = sqlite3.connect(db_path, timeout=5)
        cur = con.cursor()
        files_count = chunks_count = 0
        try: cur.execute("SELECT COUNT(*) FROM files");  files_count  = cur.fetchone()[0]
        except: pass
        try: cur.execute("SELECT COUNT(*) FROM chunks"); chunks_count = cur.fetchone()[0]
        except: pass
        con.close()
        return {"files": files_count, "chunks": chunks_count}
    except Exception as e:
        print(f"  ⚠ memory RAG stats: {e}")
        return {}

# ── Metadata push ───────────────────────────────────────────────────────────────

def push_metadata():
    if not GATEWAY_TOKEN: return
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Syncing Workspace Metadata...")

    skills   = _collect_skills()
    md_files = _collect_workspace_docs()
    version  = _openclaw_version()
    models   = _models_config()
    cron     = _cron_jobs()
    compl    = _completions_count()
    sess_ctx = _sessions_context()
    mem_rag  = _memory_rag_stats()

    print(f"  → {len(skills)} skills | {len(md_files)} docs | version={version} "
          f"| completions={compl} | mem_rag={mem_rag}")

    metadata = {
        "skills":       skills,
        "md_files":     md_files,
        "config":       {},
        "version":      version,
        "models":       models,
        "cron_jobs":    cron,
        "completions":  compl,
        "sessions_ctx": sess_ctx,
        "memory_rag":   mem_rag,
    }

    cfg_path = os.path.join(OC, "openclaw.json")
    if os.path.isfile(cfg_path):
        try:
            with open(cfg_path, 'r') as f: metadata["config"] = json.load(f)
        except: pass

    push_events([{
        "event_type": "metadata",
        "agent_type": "instance",
        "payload":    metadata,
        "timestamp":  _now(),
    }], timeout=30)

# ── SQLite pollers ──────────────────────────────────────────────────────────────

def _poll_sqlite(db_path: str, table: str, event_type: str, id_col: str = "id"):
    if not os.path.isfile(db_path):
        return  # not present yet
    seen_ids = set()
    while True:
        try:
            con = sqlite3.connect(db_path, timeout=5)
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            cur.execute(f"SELECT * FROM {table} ORDER BY rowid DESC LIMIT 200")
            rows = [dict(r) for r in cur.fetchall()]
            con.close()
            new_events = []
            for row in rows:
                rid = row.get(id_col) or row.get("run_id") or row.get("id") or str(row)
                if rid in seen_ids: continue
                seen_ids.add(rid)
                new_events.append({
                    "event_type": event_type,
                    "agent_type": "openclaw",
                    "payload":    row,
                    "timestamp":  _now(),
                })
            if new_events:
                push_events(new_events)
        except Exception as e:
            print(f"  ⚠ poll {table}: {e}")
        time.sleep(SQLITE_POLL)

def poll_task_runs():
    _poll_sqlite(os.path.join(OC, "tasks/runs.sqlite"), "task_runs", "task_run", "run_id")

def poll_flow_runs():
    _poll_sqlite(os.path.join(OC, "flows/registry.sqlite"), "flow_runs", "flow_run", "run_id")

# ── Log tailing ─────────────────────────────────────────────────────────────────

def process_line(line: str, path: str, agent_type: str):
    try:
        data = json.loads(line)
        session_id = os.path.basename(path).split('.')[0]
        event_type = "log"

        if agent_type == "openclaw":
            msg = data.get("message", {})
            if data.get("type") == "message":
                role = msg.get("role", "")
                if role == "assistant":
                    event_type = "thought"
                    for part in msg.get("content", []):
                        if part.get("type") == "toolCall": event_type = "tool_call"; break
                elif role == "user": event_type = "user_input"
            elif data.get("type") == "custom":
                event_type = data.get("customType", "custom")
        elif agent_type == "claudecode":
            event_type = data.get("type", "log")

        usage = data.get("message", {}).get("usage") or data.get("usage")
        if usage:
            usd    = usage.get("cost", {}).get("total", 0)
            tokens = usage.get("totalTokens", 0)
            if usd > 0:
                push_events([{
                    "session_id": session_id, "agent_type": agent_type,
                    "event_type": "cost",
                    "payload":    {"usd": usd, "tokens": tokens},
                    "timestamp":  _now(),
                }])

        return {
            "session_id": session_id, "agent_type": agent_type,
            "event_type": event_type, "payload": data, "timestamp": _now(),
        }
    except: return None

def tail_file(path: str, agent_type: str):
    print(f"  Monitoring: {os.path.basename(path)} ({agent_type})")
    with open(path, 'r', errors='replace') as f:
        lines = f.readlines()
        for line in lines[-10:]:
            ev = process_line(line, path, agent_type)
            if ev: push_events([ev])
        f.seek(0, 2)
        while True:
            line = f.readline()
            if not line: time.sleep(0.5); continue
            ev = process_line(line, path, agent_type)
            if ev: push_events([ev])

def monitor_dir(pattern: str, agent_type: str):
    seen = set()
    while True:
        for f in glob.glob(pattern, recursive=True):
            if f in seen: continue
            if not _is_live_session(f): continue
            Thread(target=tail_file, args=(f, agent_type), daemon=True).start()
            seen.add(f)
        time.sleep(10)

# ── System stats ────────────────────────────────────────────────────────────────

def push_stats():
    if not HAS_PSUTIL: return
    while True:
        try:
            stats = {
                "cpu_pct": psutil.cpu_percent(),
                "ram_pct": psutil.virtual_memory().percent,
            }
            push_events([{"event_type": "system", "agent_type": "instance",
                          "payload": stats, "timestamp": _now()}])
        except: pass
        time.sleep(60)

# ── Entry point ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if not GATEWAY_TOKEN:
        print("ERROR: CLAW_GATEWAY_TOKEN not set."); sys.exit(1)

    print(f"AiifyMetry Collector v5.0 - Node: {INSTANCE_ID}")
    push_metadata()

    Thread(target=monitor_dir,   args=(OPENCLAW_SESSIONS, "openclaw"),   daemon=True).start()
    Thread(target=monitor_dir,   args=(CLAUDE_LOGS,       "claudecode"), daemon=True).start()
    Thread(target=push_stats,                                             daemon=True).start()
    Thread(target=poll_task_runs,                                         daemon=True).start()
    Thread(target=poll_flow_runs,                                         daemon=True).start()

    def refresher():
        while True: time.sleep(300); push_metadata()
    Thread(target=refresher, daemon=True).start()

    while True: time.sleep(1)
