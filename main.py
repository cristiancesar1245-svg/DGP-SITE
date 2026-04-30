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
    host = "0.0.0.0"
    port = resolve_port()
    dev_reload = bool(app.config.get("DEV_RELOAD"))

    if dev_reload:
        app.run(
            host=host,
            port=port,
            debug=True,
            use_reloader=True,
        )
        raise SystemExit(0)

    try:
        from waitress import serve
    except ImportError:
        app.run(
            host=host,
            port=port,
            debug=False,
            use_reloader=False,
        )
    else:
        serve(
            app,
            host=host,
            port=port,
            threads=int(os.getenv("DGP_WAITRESS_THREADS", "8")),
        )
