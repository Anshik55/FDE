"""CLI entry point to execute data migration pipeline headless."""

import argparse
import sys
from pathlib import Path

# Add project root to sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from app.pipeline.orchestrator import run_pipeline


def main():
    parser = argparse.ArgumentParser(description="AI Agent for Client Data Migration CLI")
    parser.add_argument("--client-id", default="acme_corp", help="Client ID")
    parser.add_argument("--no-push", action="store_true", help="Skip pushing to stub target")
    args = parser.parse_args()

    print("==================================================")
    print("  AI Agent for Client Data Migration & Integration")
    print("==================================================")
    print(f"Starting pipeline run for client '{args.client_id}'...")

    result = run_pipeline(
        client_id=args.client_id,
        push_to_target=not args.no_push,
    )

    print("\n--- Migration Run Complete ---")
    print(f"Run ID:              {result['run_id']}")
    print(f"Status:              {result['status']}")
    print(f"Records Processed:   {result['records_processed']}")
    print(f"Escalations Opened:  {result['escalations_opened']}")
    print(f"Pushed to Target:    {result['push_results'].get('pushed_count', 0)}")
    print("==================================================")


if __name__ == "__main__":
    main()
