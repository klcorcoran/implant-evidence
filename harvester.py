# harvester.py
# Minimal evidence harvester for aTSA stemless (you can add features later)

import csv, datetime, requests, sys
from urllib.parse import urlencode

EMAIL = "kam.l.corcoran@gmail.com"  # used for Unpaywall & good API etiquette

# --- Helper calls ---

def pubmed_search(term, retmax=50):
    # ESearch: get PMIDs
    base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    params = {"db":"pubmed","term":term,"retmode":"json","retmax":retmax}
    r = requests.get(base, params=params, timeout=30)
    r.raise_for_status()
    return r.json().get("esearchresult",{}).get("idlist",[])

def pubmed_summary(pmids):
    # ESummary: basic metadata
    if not pmids: return []
    base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
    params = {"db":"pubmed","id":",".join(pmids),"retmode":"json"}
    r = requests.get(base, params=params, timeout=30)
    r.raise_for_status()
    data = r.json().get("result",{})
    out=[]
    for pid in pmids:
        rec = data.get(pid)
        if not rec: continue
        out.append({
            "pmid": pid,
            "title": rec.get("title",""),
            "year": rec.get("pubdate","")[:4],
            "doi": first_doi(rec),
            "link": f"https://pubmed.ncbi.nlm.nih.gov/{pid}/"
        })
    return out

def first_doi(esummary_record):
    articleids = esummary_record.get("articleids",[])
    for a in articleids:
        if a.get("idtype")=="doi":
            return a.get("value")
    return ""

def openalex_search(query, since="2018-01-01", per_page=25):
    # search reviews for our query
    base = "https://api.openalex.org/works"
    params = {
        "search": query,
        "filter": f"type:review,from_publication_date:{since}",
        "per_page": per_page
    }
    r = requests.get(base, params=params, timeout=30)
    r.raise_for_status()
    data = r.json().get("results",[])
    out=[]
    for w in data:
        doi = (w.get("doi") or "").replace("https://doi.org/","")
        out.append({
            "title": w.get("title",""),
            "year": w.get("publication_year",""),
            "doi": doi,
            "type": w.get("type",""),
            "link": f"https://doi.org/{doi}" if doi else w.get("id","")
        })
    return out

def crossref_enrich(doi):
    if not doi: return {}
    url = f"https://api.crossref.org/works/{doi}"
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        m = r.json().get("message",{})
        # funding & license if present
        funders = [f.get("name") for f in m.get("funder",[])]
        licenses = [l.get("URL") for l in m.get("license",[])]
        return {"funding": "; ".join(funders) if funders else "",
                "license_url": licenses[0] if licenses else ""}
    except Exception:
        return {}

def unpaywall_pdf(doi, email):
    if not doi: return ""
    url = f"https://api.unpaywall.org/v2/{doi}?email={email}"
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()
        best = data.get("best_oa_location") or {}
        return best.get("url_for_pdf","") or best.get("url","")
    except Exception:
        return ""

# --- Configure your first feature/outcome ---

FEATURE = "Stemless humeral component (aTSA)"
OUTCOME = "5yr_all_cause_revision"

QUERY_REV = '(stemless[Title/Abstract]) AND ("total shoulder arthroplasty"[Title/Abstract]) AND (revision OR survivorship)'

# --- Run ---

today = datetime.date.today().isoformat()

# 1) PubMed: get PMIDs + summaries
pmids = pubmed_search(QUERY_REV, retmax=50)
pm_summaries = pubmed_summary(pmids)

# 2) OpenAlex: get recent reviews/meta-analyses
oa_reviews = openalex_search("stemless anatomic total shoulder arthroplasty revision", since="2018-01-01")

# 3) Merge + enrich (Crossref, Unpaywall)
studies_rows = []
seen = set()

def add_row(card_id, study_type, title, year, doi, link, notes):
    key = (doi, link, title)
    if key in seen: return
    seen.add(key)
    enrich = crossref_enrich(doi)
    oa = unpaywall_pdf(doi, EMAIL) if doi else ""
    studies_rows.append({
        "card_id": card_id,
        "study_id": f"doi:{doi}" if doi else f"url:{link}",
        "study_type": study_type,
        "title": title,
        "year": year,
        "doi_or_link": f"https://doi.org/{doi}" if doi else link,
        "design": study_type,
        "n": "",
        "follow_up_years": "",
        "effect_measure": "",
        "effect_value": "",
        "ci_lower": "",
        "ci_upper": "",
        "funding": enrich.get("funding",""),
        "risk_of_bias_tool": "",
        "risk_of_bias_overall": "",
        "source_api": "PubMed/OpenAlex/Crossref/Unpaywall",
        "notes": notes,
        "oa_pdf": oa
    })

CARD_ID_MAIN = "card_stemless_any_5yr"

# Add OpenAlex reviews
for r in oa_reviews:
    add_row(CARD_ID_MAIN, "systematic_review_or_meta_analysis",
            r["title"], r["year"], r["doi"], r["link"],
            "SR/MA candidate (screen manually for PRISMA, AMSTAR-2).")

# Add PubMed primary studies (we'll tag generically for now)
for s in pm_summaries:
    add_row(CARD_ID_MAIN, "primary_study",
            s["title"], s["year"], s["doi"], s["link"],
            "Candidate RCT/cohort; classify later (RoB 2 / ROBINS-I).")

# 4) “Cards” CSV: a single evidence card for this feature/outcome
cards_rows = [{
    "card_id": CARD_ID_MAIN,
    "entity_type": "feature",
    "entity_name": FEATURE,
    "outcome": OUTCOME,
    "comparator": "Stemmed aTSA",
    "bottom_line": "Across SR/MA, one RCT, and adjusted registry analyses, stemless shows ~comparable 5-yr revision vs stemmed; caution with metal-backed glenoids.",
    "certainty": "moderate",
    "registry_njr": "n/a or limited shoulder coverage",
    "registry_aoanjrr": "No HTARR with all-poly glenoids; higher risk signals when metal-backed glenoids used.",
    "registry_ajrr": "Survivorship trends comparable; see Annual Report.",
    "last_updated": today,
    "tags": "shoulder;TSA;stemless"
}]

# 5) Write CSVs
with open("evidence_cards.csv","w",newline="",encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=list(cards_rows[0].keys()))
    w.writeheader(); w.writerows(cards_rows)

studies_fieldnames = ["card_id","study_id","study_type","title","year","doi_or_link","design","n",
                      "follow_up_years","effect_measure","effect_value","ci_lower","ci_upper",
                      "funding","risk_of_bias_tool","risk_of_bias_overall","source_api","notes","oa_pdf"]
with open("evidence_studies.csv","w",newline="",encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=studies_fieldnames)
    w.writeheader(); w.writerows(studies_rows)

print("Wrote evidence_cards.csv and evidence_studies.csv")
