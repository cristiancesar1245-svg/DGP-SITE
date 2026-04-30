from __future__ import annotations

import os

from python_app.app import app


def resolve_port() -> int:
    raw_port = os.getenv("PORT") or os.getenv("DGP_PORT") or "8080"
    try:
        return int(raw_port)
    except ValueError:
        return 8080


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=resolve_port(),
        debug=False,
        use_reloader=False,
    )
