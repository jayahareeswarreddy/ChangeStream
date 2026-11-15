"""Create or update the local Debezium PostgreSQL connector through Kafka Connect."""

from __future__ import annotations

import json
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
CONNECT_URL = "http://localhost:8083"
CONNECTOR_NAME = "changestream-orders"


def main() -> None:
    config = json.loads((ROOT / "connect" / "orders-connector.json").read_text())
    endpoint = f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/config"
    body = json.dumps(config).encode()

    ready = False
    for _ in range(60):
        try:
            with urlopen(f"{CONNECT_URL}/connectors", timeout=3) as response:
                ready = response.status == 200
                if ready:
                    break
        except HTTPError as error:
            if error.code < 500:
                raise SystemExit(f"Kafka Connect returned HTTP {error.code} while checking readiness.") from error
        except (TimeoutError, URLError, ConnectionError):
            pass
        time.sleep(2)

    if not ready:
        raise SystemExit("Kafka Connect did not become ready at http://localhost:8083.")

    request = Request(
        endpoint,
        data=body,
        headers={"Content-Type": "application/json"},
        method="PUT",
    )
    try:
        with urlopen(request, timeout=10) as response:
            print(f"Connector {CONNECTOR_NAME} configured (HTTP {response.status}).")
    except HTTPError as error:
        details = error.read().decode(errors="replace")
        raise SystemExit(f"Kafka Connect rejected the connector: {details}") from error


if __name__ == "__main__":
    main()
