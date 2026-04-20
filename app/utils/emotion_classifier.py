"""
emotion_classifier.py
=====================
Dual-path emotion classifier using j-hartmann/emotion-english-distilroberta-base.

- LOCAL path  : uses transformers pipeline (seeding, dev runs, no quota)
- API path    : uses HuggingFace Inference API (daily cron, production use)

Outputs 4 emotions: anger | joy | optimism | sadness

Label mapping to sentiment:
  Positive → joy, optimism
  Negative → anger, sadness
"""

import os
import time
import requests
from dotenv import load_dotenv

load_dotenv()

MODEL_ID = "cardiffnlp/twitter-roberta-base-sentiment-latest"
HF_API_URL = f"https://router.huggingface.co/hf-inference/models/{MODEL_ID}"

# ── Local inference (seeding / dev — no API key, no rate limits) ─────────────

_local_pipe = None


def _get_local_classifier():
    global _local_pipe
    if _local_pipe is None:
        from transformers import pipeline
        print(f"[emotion] Loading local model '{MODEL_ID}'... (first call only)")
        _local_pipe = pipeline(
            "text-classification",
            model=MODEL_ID,
            top_k=None,   # returns all 7 scores per text (replaces deprecated return_all_scores)
            device=-1,    # CPU
        )
        print("[emotion] Local model loaded.")
    return _local_pipe


def classify_local(texts: list[str]) -> list[dict]:
    """
    Classify a list of texts locally.
    Returns a list of dicts: [{"anger": 0.1, "joy": 0.7, ...}, ...]
    Each dict also contains "emotion_label" (dominant) and "sentiment_label".
    """
    pipe = _get_local_classifier()
    # Truncate to 512 chars — model limit is 512 tokens, ~2 chars/token avg
    safe = [str(t)[:512] if t else "" for t in texts]
    raw_results = pipe(safe, truncation=True, max_length=128, batch_size=64, top_k=None)
    return [_parse_result(r) for r in raw_results]


# ── HuggingFace Inference API (daily cron — 200–500 comments/day) ─────────────

def classify_api(texts: list[str], batch_size: int = 200) -> list[dict]:
    """
    Classify texts via HF Inference API — up to 3 batches concurrently.
    Returns same format as classify_local.
    Falls back to neutral on any API error.
    """
    hf_key = os.getenv("HUGGINGFACE_API_KEY")
    if not hf_key:
        print("[emotion] No HUGGINGFACE_API_KEY — falling back to neutral.")
        return [_neutral_result() for _ in texts]

    headers = {"Authorization": f"Bearer {hf_key}"}

    # Split into batches
    batches = []
    for i in range(0, len(texts), batch_size):
        batches.append([str(t)[:512] if t else "" for t in texts[i:i + batch_size]])

    total_batches = len(batches)
    print(f"[emotion] Classifying {len(texts)} texts via API ({total_batches} batches)")

    try:
        from app.api.reddit import fetch_progress as _fp
    except Exception:
        _fp = None

    def _classify_batch(batch_idx_and_texts):
        """Classify a single batch via HF API with retry logic."""
        batch_idx, batch = batch_idx_and_texts

        for attempt in range(4):
            try:
                resp = requests.post(
                    HF_API_URL,
                    headers=headers,
                    json={"inputs": batch, "parameters": {"top_k": None}},
                    timeout=90,
                )
                if resp.status_code == 200:
                    raw = resp.json()
                    if isinstance(raw, list) and len(raw) == 1 and isinstance(raw[0], list):
                        raw = raw[0]
                    return batch_idx, [
                        _parse_result(item) if isinstance(item, list) else _neutral_result()
                        for item in raw
                    ]
                elif resp.status_code == 503:
                    wait = 20 * (attempt + 1)
                    print(f"[emotion] HF model loading (batch {batch_idx}, attempt {attempt+1}/4) — sleeping {wait}s")
                    time.sleep(wait)
                elif resp.status_code == 429:
                    wait = 30 * (attempt + 1)
                    print(f"[emotion] HF rate limited (batch {batch_idx}, attempt {attempt+1}/4) — sleeping {wait}s")
                    time.sleep(wait)
                elif resp.status_code == 401:
                    print("[emotion] HF auth failed (401) — check HUGGINGFACE_API_KEY secret")
                    break
                else:
                    print(f"[emotion] HF error {resp.status_code}: {resp.text[:200]}")
                    break
            except requests.exceptions.Timeout:
                wait = 15 * (attempt + 1)
                print(f"[emotion] HF timeout (batch {batch_idx}, attempt {attempt+1}/4) — retrying in {wait}s")
                time.sleep(wait)
            except Exception as e:
                print(f"[emotion] Network error (batch {batch_idx}): {e}")
                time.sleep(5)

        return batch_idx, [_neutral_result() for _ in batch]

    # Send up to 3 batches concurrently
    from concurrent.futures import ThreadPoolExecutor, as_completed
    results_by_idx = {}
    completed = 0

    with ThreadPoolExecutor(max_workers=1) as ex:
        futures = {}
        for i, b in enumerate(batches):
            futures[ex.submit(_classify_batch, (i, b))] = i
            if i < len(batches) - 1:
                time.sleep(0.5)  # Throttle between requests to avoid overwhelming HF API
        for fut in as_completed(futures):
            batch_idx, batch_results = fut.result()
            results_by_idx[batch_idx] = batch_results
            completed += 1
            if _fp is not None:
                _fp["message"] = f"Analysing sentiment... ({completed}/{total_batches} batches)"

    # Reassemble in original order
    return [r for i in range(total_batches) for r in results_by_idx.get(i, [])]


# ── Shared helpers ────────────────────────────────────────────────────────────

def _parse_result(scores_list: list) -> dict:
    """
    Convert HF output list [{"label": "positive", "score": 0.85}, ...] to a flat dict.
    Normalises labels to lowercase, adds "sentiment_label" (Capitalized) and "emotion_label".
    """
    scores = {item["label"].lower(): round(float(item["score"]), 4) for item in scores_list}
    dominant = max(scores, key=scores.get)     # "positive" | "negative" | "neutral"
    sentiment = dominant.capitalize()           # "Positive" | "Negative" | "Neutral"

    return {
        **scores,
        "emotion_label":    sentiment,          # mirrors sentiment_label for backward compat
        "sentiment_label":  sentiment,
        "confidence_score": round(scores[dominant], 4),
    }


def _neutral_result() -> dict:
    return {
        "positive": 0.33, "negative": 0.33, "neutral": 0.34,
        "emotion_label":    "Neutral",
        "sentiment_label":  "Neutral",
        "confidence_score": 0.34,
    }


EMOTION_LABELS = ["positive", "negative", "neutral"]
