"""CLI entry point for the local test agent."""

import argparse
import os
import sys

from .runner import TestAgentRunner


def main():
    parser = argparse.ArgumentParser(
        description="BI Test Agent — runs Playwright tests on your local machine",
        prog="bi-test-agent",
    )
    parser.add_argument("command", choices=["connect", "run", "setup"],
                        help="connect: listen for test jobs | run: execute a single job | setup: install Edge browser")
    parser.add_argument("--server", default=os.environ.get("FUNC_APP_URL", ""),
                        help="Function App URL")
    parser.add_argument("--key", default="", help="Function App host key")
    parser.add_argument("--instance-id", default="", help="Test instance ID (for 'run' command)")
    parser.add_argument("--poll-interval", type=int, default=5, help="Seconds between polls (default: 5)")

    args = parser.parse_args()

    if args.command == "setup":
        print("Installing Playwright browsers...")
        import subprocess
        subprocess.run([sys.executable, "-m", "playwright", "install", "msedge"], check=True)
        print("Edge browser installed. Ready to run tests.")
        return

    if args.command == "connect":
        runner = TestAgentRunner(server_url=args.server, func_key=args.key)
        runner.listen(poll_interval=args.poll_interval)

    elif args.command == "run":
        if not args.instance_id:
            print("Error: --instance-id required for 'run' command")
            sys.exit(1)
        runner = TestAgentRunner(server_url=args.server, func_key=args.key)
        runner.run_single(args.instance_id)


if __name__ == "__main__":
    main()
