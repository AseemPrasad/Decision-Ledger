"""CLI Entrypoint to run the Decision Ledger B2B Control Plane Server."""

from __future__ import annotations

import argparse
import logging
import sys

from .app import create_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("decision_ledger_server")


def main() -> None:
    parser = argparse.ArgumentParser(description="Decision Ledger B2B SaaS Control Plane Server")
    parser.add_argument("--host", default="127.0.0.1", help="Host interface to bind (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8080, help="Port to listen on (default: 8080)")
    args = parser.parse_args()

    server = create_server(args.host, args.port)
    logger.info("Decision Ledger Control Plane Server listening on http://%s:%d", args.host, args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Server shutting down...")
        server.server_close()
        sys.exit(0)


if __name__ == "__main__":
    main()
