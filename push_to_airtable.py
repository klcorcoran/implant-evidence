import os, csv, json, time, requests

BASE_ID = os.environ["AIRTABLE_BASE_ID"]
TOKEN = os.environ["AIRTABLE_TOKEN"]
TABLE_CARDS = os.environ["AIRTABLE_TABLE_CARDS"]      # e.g., "Cards" or "tblXXXX"
TABLE_STUDIES = os.environ["AIRTABLE_TABLE_STUDIES"]  # e.g., "Studies" or "tblYYYY"

API = "https://api.airtable.com/v0"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# Airtable limits: max 10 records per create/update request; ~5 req/sec per base
# We'll batch in 10s and sleep ~0.35s between requests to stay under the rate. 
# Docs: rate limits + create records. 
# (If you ever hit 429, back off ~30s and retry.)
def batched(iterable, n=10):
    batch=[]
    for item in iterable:
        batch.append(item)
        if len(batch)==n:
            yield batch; batch=[]
    if batch:
        yield batch

def upsert(table, records, merge_field):
    # PATCH /v0/{base}/{table} with performUpsert merges on your key (e.g., 'study_id')
    url = f"{API}/{BASE_ID}/{table}"
    for chunk in batched(records, n=10):  # <=10 per request
        body = {
            "performUpsert": {"fieldsToMergeOn": [merge_field]},
            "records": [{"fields": r} for r in chunk]
        }
        r = requests.patch(url, headers=HEADERS, data=json.dumps(body))
        if r.status_code == 429:
            # hit rate limit: wait and retry once
            time.sleep(30)
            r = requests.patch(url, headers=HEADERS, data=json.dumps(body))
        if r.status_code >= 300:
            raise SystemExit(f"Airtable upsert error {r.status_code}: {r.text}")
        time.sleep(0.35)

def load_csv(path):
    out=[]
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            out.append(row)
    return out

def main():
    cards = load_csv("evidence_cards.csv")
    studies = load_csv("evidence_studies.csv")

    # Ensure our merge keys exist as fields in Airtable
    # Upsert by 'card_id' for the Cards table:
    upsert(TABLE_CARDS, cards, merge_field="card_id")

    # Upsert by 'study_id' for the Studies table:
    upsert(TABLE_STUDIES, studies, merge_field="study_id")

    print("Airtable push complete.")

if __name__ == "__main__":
    main()
