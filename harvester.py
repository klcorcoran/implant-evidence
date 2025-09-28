# harvester.py (resilient)
import csv, datetime, requests

EMAIL = "kam.l.corcoran@gmail.com"

def safe_get(url, params=None, timeout=30):
    try:
        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r
    except Exception as e:
        print(f"[warn] GET failed: {url}  ({e})")
        return None

def pubmed_search(term, retmax=50):
    r = safe_get("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
                 {"db":"pubmed","term":term,"retmode":"json","retmax":retmax})
    return (r.json().get("esearchresult",{}).get("idlist",[]) if r else [])

def pubmed_summary(pmids):
    if not pmids: return []
    r = safe_get("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi",
                 {"db":"pubmed","id":",".join(pmids),"retmode":"json"})
    if not r: return []
    data = r.json().get("result",{})
    out=[]
    for pid in pmids:
        rec = data.get(pid) or {}
        doi = ""
        for a in rec.get("articleids",[]):
            if a.get("idtype")=="doi":
                doi = a.get("value",""); break
        out.append({
            "pmid": pid,
            "title": rec.get("title",""),
            "year": (rec.get("pubdate","")[:4] or ""),
            "doi": doi,
            "link": f"https://pubmed.ncbi.nlm.nih.gov/{pid}/"
        })
    return out

def openalex_reviews(query, since="2018-01-01", per_page=25):
    r = safe_get("https://api.openalex.org/works",
                 {"search": query, "filter": f"type:review,from_publication_date:{since}", "per_page": per_page})
    if not r: return []
    out=[]
    for w in r.json().get("results",[]):
        doi = (w.get("doi") or "").replace("https://doi.org/","")
        out.append({
            "title": w.get("title",""),
            "year": str(w.get("publication_year","")),
            "doi": doi,
            "link": (f"https://doi.org/{doi}" if doi else (w.get("id","") or ""))
        })
    return out

def crossref_enrich(doi):
    if not doi: return {}
    r = safe_get(f"https://api.crossref.org/works/{doi}", timeout=20)
    if not r: return {}
    m = r.json().get("message",{})
    funders = [f.get("name") for f in m.get("funder",[])]
    licenses = [l.get("URL") for l in m.get("license",[])]
    return {"funding": "; ".join(funders) if funders else "", "license_url": (licenses[0] if licenses else "")}

def unpaywall_pdf(doi, email):
    if not doi: return ""
    r = safe_get(f"https://api.unpaywall.org/v2/{doi}?email={email}", timeout=20)
    if not r: return ""
    data = r.json()
    best = data.get("best_oa_location") or {}
    return best.get("url_for_pdf","") or best.get("url","") or ""

# ---- your first target (primary aTSA, stemless) ----
FEATURE = "Stemless humeral component (aTSA)"
OUTCOME = "5yr_all_cause_revision"
QUERY = '(stemless[Title/Abstract]) AND ("total shoulder arthroplasty"[Title/Abstract]) AND (revision OR survivorship)'
today = datetime.date.today().isoformat()

pmids = pubmed_search(QUERY, retmax=50)
pm = pubmed_summary(pmids)
oa = openalex_reviews('stemless anatomic total shoulder arthroplasty revision', since="2018-01-01")

studies_rows, seen = [], set()
def add_row(card_id, study_type, title, year, doi, link, note):
    key = (doi or "", link or "", title or "")
    if key in seen: return
    seen.add(key)
    enrich = crossref_enrich(doi)
    pdf = unpaywall_pdf(doi, EMAIL) if doi else ""
    studies_rows.append({
        "card_id": card_id,
        "study_id": f"doi:{doi}" if doi else f"url:{link}",
        "study_type": study_type,
        "title": title or "",
        "year": year or "",
        "doi_or_link": (f"https://doi.org/{doi}" if doi else (link or "")),
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
        "notes": note,
        "oa_pdf": pdf
    })

CARD_ID = "card_stemless_any_5yr"
for r in oa:
    add_row(CARD_ID, "systematic_review_or_meta_analysis", r["title"], r["year"], r["doi"], r["link"],
            "SR/MA candidate (PRISMA/AMSTAR-2 to assess).")
for s in pm:
    add_row(CARD_ID, "primary_study", s["title"], s["year"], s["doi"], s["link"],
            "Candidate RCT/cohort; classify later (RoB 2 / ROBINS-I).")

cards_rows = [{
    "card_id": CARD_ID,
    "entity_type": "feature",
    "entity_name": FEATURE,
    "outcome": OUTCOME,
    "comparator": "Stemmed aTSA",
    "bottom_line": "Across SR/MA, one RCT, and adjusted registry analyses, stemless shows ~comparable 5-yr revision vs stemmed; caution with metal-backed glenoids.",
    "certainty": "moderate",
    "registry_njr": "n/a or limited shoulder coverage",
    "registry_aoanjrr": "No HTARR with all-poly glenoids; higher risk signals with metal-backed glenoids.",
    "registry_ajrr": "Survivorship trends comparable; see Annual Report.",
    "last_updated": today,
    "tags": "shoulder;TSA;stemless"
}]

with open("evidence_cards.csv","w",newline="",encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=list(cards_rows[0].keys()))
    w.writeheader(); w.writerows(cards_rows)

stud_fields = ["card_id","study_id","study_type","title","year","doi_or_link","design","n","follow_up_years",
               "effect_measure","effect_value","ci_lower","ci_upper","funding","risk_of_bias_tool",
               "risk_of_bias_overall","source_api","notes","oa_pdf"]
with open("evidence_studies.csv","w",newline="",encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=stud_fields)
    w.writeheader(); w.writerows(studies_rows)

print("Done. Created evidence_cards.csv and evidence_studies.csv")
