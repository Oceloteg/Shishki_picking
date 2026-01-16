"""Compatibility wrapper for the OData probe.

The project uses the probe script as a runnable module:

    python -m scripts.onec_probe

Historically there was also a module under app.onec, but it drifted and could
break due to outdated imports. Keep this thin wrapper so any old calls like:

    python -m app.onec.onec_probe

continue to work and always run the up-to-date probe implementation.
"""

from __future__ import annotations


async def main() -> int:
    from scripts.onec_probe import main as _main

    return await _main()


if __name__ == "__main__":
    import asyncio

    raise SystemExit(asyncio.run(main()))
