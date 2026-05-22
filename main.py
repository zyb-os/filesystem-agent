"""
main.py — Entry point for the filesystem-agent.

Usage:
    python main.py [--orchestrator-url http://localhost:8000]

Environment:
    LOG_LEVEL   — logging level (default: INFO)
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os

from orchestrator_client import OrchestratorClient

logging.basicConfig(
    level=logging.getLevelName(os.environ.get("LOG_LEVEL", "INFO").upper()),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Filesystem Agent")
    p.add_argument(
        "--orchestrator-url",
        default=os.environ.get("ORCHESTRATOR_URL", "http://localhost:8000"),
        help="Base URL of the agent orchestrator (default: http://localhost:8000)",
    )
    return p.parse_args()


async def main() -> None:
    args = parse_args()
    logger.info("Starting filesystem-agent → %s", args.orchestrator_url)
    client = OrchestratorClient(orchestrator_url=args.orchestrator_url)
    await client.start()


if __name__ == "__main__":
    asyncio.run(main())
