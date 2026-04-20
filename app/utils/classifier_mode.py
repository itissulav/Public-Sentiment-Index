"""
app/utils/classifier_mode.py
============================
Single source of truth for which sentiment classifier backend to use.

- Deployed (Cloud Run) → 'api'  (transformers not in requirements.txt)
- Local dev            → 'local' (transformers pipeline, no API quota)

Controlled by the CLASSIFIER_MODE env var, with a hard Cloud Run
safety guard that wins over any explicit override.
"""
import os


def get_classifier_mode() -> str:
    """Return 'local' or 'api' based on environment."""
    # Hard guard: Cloud Run injects K_SERVICE. Never attempt to load
    # the local model in production — transformers is not installed.
    if os.getenv("K_SERVICE"):
        return "api"

    explicit = os.getenv("CLASSIFIER_MODE", "").strip().lower()
    if explicit in ("local", "api"):
        return explicit

    # Default when nothing set: api (safest)
    return "api"
