#!/usr/bin/env python3
import subprocess
import os
import sys

def main():
    print("Starting Streamlit GaC UI...")
    app_path = os.path.join(os.path.dirname(__file__), "src", "ui", "app.py")
    if not os.path.exists(app_path):
        print(f"Error: Could not find {app_path}")
        sys.exit(1)
        
    # Set PYTHONPATH so src modules are resolvable if needed
    env = os.environ.copy()
    env["PYTHONPATH"] = os.path.join(os.path.dirname(__file__), "src")
    
    cmd = [sys.executable, "-m", "streamlit", "run", app_path]
    try:
        subprocess.run(cmd, env=env)
    except KeyboardInterrupt:
        print("\nStopping Streamlit UI...")

if __name__ == "__main__":
    main()
