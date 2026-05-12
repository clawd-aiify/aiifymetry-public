import os
import sys
import subprocess
import time
import json
import glob
from datetime import datetime
from threading import Thread

# --- BOOTSTRAP DEPENDENCIES ---
def install_dependencies():
    required = ["requests", "psutil"]
    
    pip_available = True
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "--version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError:
        pip_available = False
        
    if not pip_available:
        print("pip not found. Attempting to bootstrap pip with ensurepip...")
        try:
            subprocess.check_call([sys.executable, "-m", "ensurepip", "--user"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            pip_available = True
        except:
            print("CRITICAL: pip is not installed and ensurepip failed.")

    for package in required:
        try:
            __import__(package)
        except ImportError:
            if pip_available:
                print(f"Installing missing dependency: {package}...")
                try:
                    subprocess.check_call([sys.executable, "-m", "pip", "install", package, "--quiet"])
                except: pass

install_dependencies()

try:
    import requests
except ImportError:
    print("CRITICAL: 'requests' missing. Telemetry disabled.")
    sys.exit(1)

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False
# ------------------------------

# Configuration
INGESTOR_URL = "https://aiifymetry-34805915210.us-central1.run.app"
GATEWAY_TOKEN = (os.getenv("CLAW_GATEWAY_TOKEN") or "").strip()
INSTANCE_ID = os.getenv("CLAW_INSTANCE_ID", "new-instance")
CUSTOMER_ID = os.getenv("CLAW_CUSTOMER_ID", "default")

# Log Paths
OPENCLAW_SESSIONS = os.path.expanduser("~/.openclaw/agents/main/sessions/*.jsonl")
CLAUDE_LOGS = os.path.expanduser("~/.claude/projects/**/*.jsonl")
OPENCLAW_CONFIG_PATH = os.path.expanduser("~/.openclaw/openclaw.json")
SKILLS_DIR = os.path.expanduser("~/.hermes/skills/")

def push_events(events):
    if not events or not GATEWAY_TOKEN: return
    payload = {"instance_id": INSTANCE_ID, "customer_id": CUSTOMER_ID, "events": events}
    try:
        requests.post(f"{INGESTOR_URL}/ingest", json=payload, 
                      headers={"X-Gateway-Token": GATEWAY_TOKEN}, timeout=5)
    except: pass

def push_metadata():
    """Push configuration, skills, and identity files."""
    if not GATEWAY_TOKEN: return
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Syncing Workspace Metadata...")
    metadata = {"config": {}, "skills": [], "md_files": []}
    
    # 1. OpenClaw Config
    if os.path.exists(OPENCLAW_CONFIG_PATH):
        try:
            with open(OPENCLAW_CONFIG_PATH, 'r') as f: metadata["config"] = json.load(f)
        except: pass
        
    # 2. Skills Discovery
    if os.path.exists(SKILLS_DIR):
        for item in os.listdir(SKILLS_DIR):
            path = os.path.join(SKILLS_DIR, item)
            if os.path.isdir(path): metadata["skills"].append({"name": item})
                
    # 3. Workspace Docs Discovery (Aggressive Search)
    search_paths = [os.getcwd(), os.path.expanduser("~/.openclaw")]
    seen_files = set()
    for root_path in search_paths:
        if not os.path.exists(root_path): continue
        # Find all .md files up to 2 levels deep
        for root, dirs, files in os.walk(root_path):
            depth = root[len(root_path):].count(os.sep)
            if depth > 2: continue
            for f in files:
                if f.endswith(".md") and f not in seen_files:
                    metadata["md_files"].append({"name": f, "path": os.path.join(root, f)})
                    seen_files.add(f)

    push_events([{"event_type": "metadata", "agent_type": "instance", "payload": metadata, "timestamp": datetime.utcnow().isoformat()}])

def process_line(line, path, agent_type):
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
            elif data.get("type") == "custom": event_type = data.get("customType", "custom")
        elif agent_type == "claudecode":
            event_type = data.get("type", "log")

        # Cost tracking
        usage = data.get("message", {}).get("usage") or data.get("usage")
        if usage:
            usd = usage.get("cost", {}).get("total", 0)
            tokens = usage.get("totalTokens", 0)
            if usd > 0:
                push_events([{"session_id": session_id, "agent_type": agent_type, "event_type": "cost", 
                             "payload": {"usd": usd, "tokens": tokens}, "timestamp": datetime.utcnow().isoformat()}])

        return {"session_id": session_id, "agent_type": agent_type, "event_type": event_type, "payload": data, "timestamp": datetime.utcnow().isoformat()}
    except: return None

def tail_file(path, agent_type):
    print(f"Monitoring: {os.path.basename(path)} ({agent_type})")
    with open(path, 'r', errors='replace') as f:
        # BACKFILL: Send last 10 lines immediately
        lines = f.readlines()
        for line in lines[-10:]:
            ev = process_line(line, path, agent_type)
            if ev: push_events([ev])
            
        f.seek(0, 2) # Move to end for live tailing
        while True:
            line = f.readline()
            if not line:
                time.sleep(0.5); continue
            ev = process_line(line, path, agent_type)
            if ev: push_events([ev])

def monitor_dir(pattern, agent_type):
    seen = set()
    while True:
        for f in glob.glob(pattern, recursive=True):
            if ".trajectory." in f: continue
            if f not in seen:
                Thread(target=tail_file, args=(f, agent_type), daemon=True).start()
                seen.add(f)
        time.sleep(10)

def push_stats():
    if not HAS_PSUTIL: return
    while True:
        try:
            stats = {"cpu_pct": psutil.cpu_percent(), "ram_pct": psutil.virtual_memory().percent}
            push_events([{"event_type": "system", "agent_type": "instance", "payload": stats, "timestamp": datetime.utcnow().isoformat()}])
        except: pass
        time.sleep(60)

if __name__ == "__main__":
    if not GATEWAY_TOKEN:
        print("ERROR: CLAW_GATEWAY_TOKEN not set."); sys.exit(1)
    
    print(f"AiifyMetry Collector v4.3 - Node: {INSTANCE_ID}")
    push_metadata()
    Thread(target=monitor_dir, args=(OPENCLAW_SESSIONS, "openclaw"), daemon=True).start()
    Thread(target=monitor_dir, args=(CLAUDE_LOGS, "claudecode"), daemon=True).start()
    Thread(target=push_stats, daemon=True).start()
    
    # Periodic metadata refresh (skills/docs)
    def refresher():
        while True: time.sleep(300); push_metadata()
    Thread(target=refresher, daemon=True).start()
    
    while True: time.sleep(1)
