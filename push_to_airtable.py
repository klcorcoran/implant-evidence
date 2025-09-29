import os, sys, csv, json, time, requests

# --------- Config from GitHub Secrets ----------
BASE_ID        = os.getenv("AIRTABLE_BASE_ID")
TOKEN          = os.getenv("AIRTABLE_TOKEN")
TABLE_CARDS    = os.getenv("AIRTABLE_TABLE_CARDS")      # e.g., "Cards" or "tblXXXX"
TABLE_STUDIES  = os.getenv("AIRTABLE_TABLE_STUDIES")    # e.g., "Studies" or "tblYYYY"

missing = [k for k,v in {
    "AIRTABLE_BASE_ID": BASE_ID,
    "AIRTABLE_TOKEN": TOKEN,
    "AIRTABLE_TABLE_CARDS": TABLE_CARDS,
    "AIRTABLE_TABLE_STUDIES": TABLE_STUDIES
}.items() if not v]
if missing:
    sys.exit(f"Missing required env vars: {', '.join(missing)}")

API = "https://api.airtable.com/v0"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# --------- Helpers (limits: ≤10 recs/request; ~5 req/s per base) ---------
# Official rate limits: 5 req/s per base. Bulk create/update limit = 10 records per request. 
# We'll sleep a bit between requests to stay under the cap. 
# Docs: rate limits & bulk update/upsert. 
# https://www.airtable.com/developers/web/api/rate-limits
# https://www.airtable.com/developers/web/api/update-multiple-records

def batched(iterable, n=10):
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) == n:
            yield batch
            batch = []
    if batch:
        yield batch

def load_csv(path):
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows.append(row)
    return rows

def dedupe_by_key(rows, key):
    """Keep only one row per unique key; drop blanks."""
    seen = {}
    for r in rows:
        k = (r.get(key) or "").strip()
        if not k:
            continue
        seen[k] = r  # last write wins
    return list(seen.values())

def upsert(table, records, merge_field):
    """PATCH with performUpsert so we update-or-create by a unique key."""
    url = f"{API}/{BASE_ID}/{table}"
    for chunk in batched(records, n=10):
        body = {
            "performUpsert": {"fieldsToMergeOn": [merge_field]},
            "records": [{"fields": r} for r in chunk],
        }
        r = requests.patch(url, headers=HEADERS, data=json.dumps(body))
        if r.status_code == 429:
            time.sleep(30); r = requests.patch(url, headers=HEADERS, data=json.dumps(body))
        if r.status_code >= 300:
            print("Airtable upsert error", r.status_code, r.text)
            raise SystemExit(1)
        time.sleep(0.35)

def list_records(table, fields=None, page_size=100):
    """Return all records (id + requested fields)."""
    url = f"{API}/{BASE_ID}/{table}"
    out, offset = [], None
    while True:
        params = {"pageSize": page_size}
        if fields:
            params["fields[]"] = fields  # Airtable supports repeated fields[]
        if offset:
            params["offset"] = offset
        r = requests.get(url, headers=HEADERS, params=params)
        r.raise_for_status()
        j = r.json()
        out.extend(j.get("records", []))
        offset = j.get("offset")
        if not offset:
            break
        time.sleep(0.25)
    return out

def batch_update(table, recs):
    """PATCH multiple records by id (≤10 per request)."""
    url = f"{API}/{BASE_ID}/{table}"
    for chunk in batched(recs, n=10):
        body = {"records": chunk}
        r = requests.patch(url, headers=HEADERS, data=json.dumps(body))
        if r.status_code == 429:
            time.sleep(30); r = requests.patch(url, headers=HEADERS, data=json.dumps(body))
        if r.status_code >= 300:
            print("Airtable link update error", r.status_code, r.text)
            raise SystemExit(1)
        time.sleep(0.35)

# --------- Main ---------
def main():
    # 1) Read + tidy CSVs we just generated
    cards   = dedupe_by_key(load_csv("evidence_cards.csv"),   "card_id")
    studies = dedupe_by_key(load_csv("evidence_studies.csv"), "study_id")

    # 2) Upsert into Airtable by unique text keys
    #    (Your tables must have text fields named card_id / study_id.)
    upsert(TABLE_CARDS,   cards,   merge_field="card_id")
    upsert(TABLE_STUDIES, studies, merge_field="study_id")
    print("Upsert complete.")

    # 3) Build a map: card_id (text) -> recordId (Card table)
    #    We'll use this to fill the *linked* field in Studies (named "Card").
    cards_idx = {}
    for rec in list_records(TABLE_CARDS, fields=["card_id"]):
        cid = (rec.get("fields", {}).get("card_id") or "").strip()
        if cid:
            cards_idx[cid] = rec["id"]

    # 4) Link Studies → Cards by recordId (Linked fields must be an array of record IDs)
    #    https://www.airtable.com/developers/web/api/field-model  (linked records are arrays of record IDs)
    to_link = []
    for rec in list_records(TABLE_STUDIES, fields=["study_id", "card_id", "Card"]):
        fields = rec.get("fields", {})
        if fields.get("Card"):    # already linked
            continue
        cid = (fields.get("card_id") or "").strip()
        if not cid or cid not in cards_idx:
            continue
        to_link.append({"id": rec["id"], "fields": {"Card": [{"id": cards_idx[cid]}]}})

    if to_link:
        batch_update(TABLE_STUDIES, to_link)
        print(f"Linked {len(to_link)} studies to cards.")
    else:
        print("No links needed.")

if __name__ == "__main__":
    main()
