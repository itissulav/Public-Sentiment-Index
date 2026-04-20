"""
app/utils/topic_image.py
========================
Resolves a topic name to a PNG filename under app/static/images/.

Lookup order:
  1. PREDEFINED_TOPIC_IMAGES dict (explicit override, from constants.py)
  2. Slug of topic name — e.g. "The Boys" → "theboys.png" (admin convention)
  3. Default placeholder image

No DB schema change required. Admin simply drops a matching {slug}.png into
app/static/images/ when they add a new predefined topic.
"""

import os
import re
from app.utils.constants import PREDEFINED_TOPIC_IMAGES


_STATIC_IMAGES_DIR = os.path.join(os.path.dirname(__file__), "..", "static", "images")
_DEFAULT_IMAGE     = "placeholder-topic.png"


def _slug(name: str) -> str:
    """Lowercase + strip non-alphanumeric — matches PREDEFINED_TOPIC_IMAGES keys."""
    return re.sub(r"[^a-z0-9]", "", name.lower())


def get_topic_image_filename(topic_name: str) -> str:
    """Return the filename (not full URL) under /static/images/ for a topic."""
    if topic_name in PREDEFINED_TOPIC_IMAGES:
        return PREDEFINED_TOPIC_IMAGES[topic_name]

    candidate = f"{_slug(topic_name)}.png"
    if os.path.exists(os.path.join(_STATIC_IMAGES_DIR, candidate)):
        return candidate

    return _DEFAULT_IMAGE
