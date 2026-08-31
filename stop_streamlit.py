#!/usr/bin/env python3
import subprocess
import sys

def main():
    print("Stopping Streamlit processes...")
    try:
        # Find all streamlit processes and kill them
        # pkill -f "streamlit run"
        subprocess.run(["pkill", "-f", "streamlit run"], check=True)
        print("Successfully stopped Streamlit GaC UI.")
    except subprocess.CalledProcessError:
        print("No running Streamlit processes found.")
    except Exception as e:
        print(f"Error while stopping Streamlit: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
