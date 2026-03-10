import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

HUGGINGFACE_API_KEY = os.getenv("HUGGINGFACE_API_KEY")
MODEL_ID = "cardiffnlp/twitter-roberta-base-sentiment"
API_URL = f"https://router.huggingface.co/hf-inference/models/{MODEL_ID}"
HEADERS = {"Authorization": f"Bearer {HUGGINGFACE_API_KEY}"}

batch_texts = ["I love this product!", "This is terrible."]
payload = {
    "inputs": batch_texts
}

response = requests.post(API_URL, headers=HEADERS, json=payload)
results = response.json()

predictions_list = []
if not results:
    predictions_list = None
elif len(results) == len(batch_texts):
    predictions_list = results
elif len(results) == 1 and isinstance(results[0], list) and len(results[0]) == len(batch_texts):
    predictions_list = [[p] for p in results[0]]

print(json.dumps(predictions_list, indent=4))
