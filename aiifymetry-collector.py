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
    
    # Check if pip is available
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
            print("Please run: sudo apt update && sudo apt install -y python3-pip")

    for package in required:
        try:
            __import__(package)
        except ImportError:
            if pip_available:
                print(f"Installing missing dependency: {package}...")
                try:
                    subprocess.check_call([sys.executable, "-m", "pip", "install", package, "--quiet"])
                except Exception as e:
                    print(f"Failed to install {package}: {e}")
            else:
                print(f"Skipping {package} (pip missing).")

# Run bootstrap
install_dependencies()

# Attempt imports with graceful fallbacks
try:
    import requests
except ImportError:
    print("CRITICAL: 'requests' library is required. Please install it manually: pip install requests")
    sys.exit(1)

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    print("Warning: 'psutil' missing. Hardware metrics will be disabled.")
    HAS_PSUTIL = False
# ------------------------------

# Configuration
INGESTOR_URL = "https://aiifymetry-34805915210.us-central1.run.app"
GATEWAY_TOKEN = os.getenv("CLAW_GATEWAY_TOKEN")
INSTANCE_ID = os.getenv("CLAW_INSTANCE_ID", "new-instance")
CUSTOMER_ID = os.getenv("CLAW_CUSTOMER_ID", "default")

# Log Paths
OPENCLAW_SESSIONS = os.path.expanduser("~/.openclaw/agents/main/sessions/*.jsonl")
CLAUDE_LOGS = os.path.expanduser("~/.claude/projects/**/*.jsonl")
OPENCLAW_CONFIG = os.path.expanduser("~/.openclaw/openclaw.json")
SKILLS_DIR = os.path.expanduser("~/.hermes/skills/")

def push_events(events):
    if not events or not GATEWAY_TOKEN:
        return
    payload = {
        "instance_id": INSTANCE_ID,
        "customer_id": CUSTOMER_ID,
        "events": events
    }
    try:
        response = requests.post(
            f"{INGESTOR_URL}/ingest",
            json=payload,
            headers={"X-Gateway-Token": GATEWAY_TOKEN},
            timeout=5
        )
        if response.status_code != 200:
            print(f"Failed to push events: {response.text}")
    except Exception as e:
        print(f"Error pushing events: {e}")

def push_metadata():
    """Push configuration, skills, and identity files."""
    if not GATEWAY_TOKEN:
        print("Error: CLAW_GATEWAY_TOKEN not set. Metadata sync skipped.")
        return
        
    print("Pushing instance metadata...")
    metadata = {
        "config": {},
        "skills": [],
        "md_files": []
    }
    
    # Load OpenClaw Config
    if os.path.exists(OPENCLAW_CONFIG):
        try:
            with open(OPENCLAW_CONFIG, 'r') as f:
                metadata["config"] = json.load(f)
        except: pass
        
    # List Skills
    if os.path.exists(SKILLS_DIR):
        for skill_dir in glob.glob(os.path.join(SKILLS_DIR, "*")):
            if os.path.isdir(skill_dir):
                metadata["skills"].append({"name": os.path.basename(skill_dir)})
                
    # List Identity Docs (Local Directory)
    for md_file in glob.glob("*.md"):
        metadata["md_files"].append({"name": md_file})

    push_events([{
        "event_type": "metadata",
        "agent_type": "instance",
        "payload": metadata,
        "timestamp": datetime.utcnow().isoformat()
    }])

def tail_file(path, agent_type, processor_func):
    """Tail a file and process new lines."""
    print(f"Tailing {path} ({agent_type})...")
    with open(path, 'r', errors='replace') as f:
        f.seek(0, 2)
        while True:
            line = f.readline()
            if not line:
                time.sleep(0.5)
                continue
            
            event = processor_func(line, path)
            if event:
                push_events([event])

def process_generic_jsonl(line, path, agent_type):
    try:
        data = json.loads(line)
        session_id = os.path.basename(path).split('.')[0]
        
        event_type = "log"
        if agent_type == "claudecode":
            event_type = data.get("type", "unknown")
        elif agent_type == "openclaw":
            msg = data.get("message", {})
            if data.get("type") == "message":
                role = msg.get("role", "")
                if role == "assistant":
                    event_type = "thought"
                    for part in msg.get("content", []):
                        if part.get("type") == "toolCall":
                            event_type = "tool_call"
                            break
                elif role == "user":
                    event_type = "user_input"
                elif role == "toolResult":
                    event_type = "tool_output"
            elif data.get("type") == "custom":
                event_type = data.get("customType", "custom")
        
        event = {
            "session_id": session_id,
            "agent_type": agent_type,
            "event_type": event_type,
            "payload": data,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Cost extraction
        usage = None
        if agent_type == "openclaw" and data.get("type") == "message" and msg.get("role") == "assistant":
            usage = msg.get("usage")
        elif agent_type == "claudecode" and data.get("type") == "assistant":
            usage = data.get("message", {}).get("usage", {})
            
        if usage:
            cost = usage.get("cost", {}).get("total", 0)
            if cost == 0:
                in_t = usage.get("input_tokens", usage.get("input", 0))
                out_t = usage.get("output_tokens", usage.get("output", 0))
                cost = (in_t * 3 / 1_000_000) + (out_t * 15 / 1_000_000)
            
            push_events([{
                "session_id": session_id,
                "agent_type": agent_type,
                "event_type": "cost",
                "payload": {"usd": cost, "tokens": usage.get("totalTokens", 0), "model": msg.get("model", data.get("message", {}).get("model"))},
                "timestamp": datetime.utcnow().isoformat()
            }])
            
        return event
    except:
        pass
    return None

def collect_system_metrics():
    """Periodically push CPU/RAM usage."""
    if not HAS_PSUTIL: return
    while True:
        try:
            metrics = {
                "cpu_pct": psutil.cpu_percent(),
                "ram_pct": psutil.virtual_memory().percent,
                "disk_pct": psutil.disk_usage('/').percent
            }
            push_events([{
                "event_type": "system",
                "agent_type": "instance",
                "payload": metrics,
                "timestamp": datetime.utcnow().isoformat()
            }])
        except Exception as e:
            print(f"System metrics error: {e}")
        time.sleep(60)

def monitor_directory(pattern, agent_type, processor):
    seen_files = set()
    while True:
        files = glob.glob(pattern, recursive=True)
        for f in files:
            if ".trajectory." in f: continue
            if f not in seen_files:
                Thread(target=tail_file, args=(f, agent_type, processor), daemon=True).start()
                seen_files.add(f)
        time.sleep(5)

if __name__ == "__main__":
    if not GATEWAY_TOKEN:
        print("CRITICAL ERROR: CLAW_GATEWAY_TOKEN environment variable is not set.")
        exit(1)

    print(f"AiifyMetry Collector v4 started for {INSTANCE_ID} / {CUSTOMER_ID}")
    
    # Push initial metadata
    push_metadata()
    
    # Start monitors
    Thread(target=monitor_directory, args=(OPENCLAW_SESSIONS, "openclaw", lambda l, p: process_generic_jsonl(l, p, "openclaw")), daemon=True).start()
    Thread(target=monitor_directory, args=(CLAUDE_LOGS, "claudecode", lambda l, p: process_generic_jsonl(l, p, "claudecode")), daemon=True).start()
    Thread(target=collect_system_metrics, daemon=True).start()
    
    # Push metadata periodically
    def periodic_metadata():
        while True:
            time.sleep(600)
            push_metadata()
    Thread(target=periodic_metadata, daemon=True).start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping...")
