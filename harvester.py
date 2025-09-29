# harvester.py — config-driven (PubMed + OpenAlex + Crossref + Unpaywall) with SR/MA boost
import csv, datetime, time, requests, sys, os

EMAIL = os.getenv("HARVESTER_CONTACT_EMAIL", "kam.l.corcoran@gmail.com")  # real email for Unpaywall
OPENALEX_SINCE = os.getenv("OPENALEX_SINCE", "2018-01-01")
PUBMED_RETMAX = int(os.getenv("PUBMED_RETMAX", "50"))
USER_AGENT = "implant-evidence-harvester/1.1 (+https://example.com; contact=" + EMAIL + ")"
HEADERS = {"User-Agent": USER_AGENT}

# ---------- HTTP ----------
def safe_get(url, params=None, timeout=30):
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        return r
    except Exception as e:
        print(f"[warn] GET failed: {url} ({e})")
        return None

# ---------- PubMed (NCBI E-utilities) ----------
def pubmed_search(term, retmax=50):
    # ESearch returns PMIDs for a query
    r = safe_get("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
                 {"db":"pubmed","term":term,"retmode":"json","retmax":retmax})
    return (r.json().get("esearchresult",{}).get("idlist",[]) if r else [])

def pubmed_summary(pmids):
    # ESummary returns brief metadata per PMID
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
        pubdate = rec.get("pubdate","") or ""
        year = pubdate[:4] if pubdate else ""
        out.append({
            "pmid": pid,
            "title": rec.get("title",""),
            "year": year,
            "doi": doi,
            "link": f"https://pubmed.ncbi.nlm.nih.gov/{pid}/"
        })
    return out

# Boost: PubMed SR/MA so you don't miss reviews indexed in Medline
def pubmed_systematic_reviews(term, retmax=50):
    # (a) PubMed Systematic Review subset
    q1 = f"({term}) AND systematic[sb]"
    # (b) Publication type tags for meta-analysis / systematic review
    q2 = f"({term}) AND (meta-analysis[Publication Type] OR systematic review[Publication Type])"
    ids = list(dict.fromkeys(pubmed_search(q1, retmax) + pubmed_search(q2, retmax)))
    return pubmed_summary(ids)

# ---------- OpenAlex (works: type:review + since) ----------
def openalex_reviews(query, since="2018-01-01", per_page=25):
    r = safe_get("https://api.openalex.org/works",
                 {"search": query, "filter": f"type:review,from_publication_date:{since}", "per_page": per_page})
    if not r: return []
    out=[]
    for w in r.json().get("results",[]):
        doi = (w.get("doi") or "").replace("https://doi.org/","")
        out.append({
            "title": w.get("title",""),
            "year": str(w.get("publication_year","") or ""),
            "doi": doi,
            "link": (f"https://doi.org/{doi}" if doi else (w.get("id","") or ""))
        })
    return out

# ---------- Crossref + Unpaywall ----------
def crossref_enrich(doi):
    if not doi: return {}
    r = safe_get(f"https://api.crossref.org/works/{doi}", timeout=20)
    if not r: return {}
    m = r.json().get("message",{})
    funders = [f.get("name") for f in m.get("funder",[]) if f.get("name")]
    licenses = [l.get("URL") for l in m.get("license",[]) if l.get("URL")]
    return {"funding": "; ".join(funders) if funders else "", "license_url": (licenses[0] if licenses else "")}

def unpaywall_pdf(doi, email):
    if not doi: return ""
    r = safe_get(f"https://api.unpaywall.org/v2/{doi}", params={"email": email}, timeout=20)
    if not r: return ""
    best = (r.json() or {}).get("best_oa_location") or {}
    return best.get("url_for_pdf","") or best.get("url","") or ""

# ---------- Row builders ----------
def add_study_row(studies_rows, seen, card_id, study_type, title, year, doi, link, note, email):
    key = (doi or "", link or "", title or "", study_type or "", card_id or "")
    if key in seen: return
    seen.add(key)
    enrich = crossref_enrich(doi)
    pdf = unpaywall_pdf(doi, email) if doi else ""
    studies_rows.append({
        "card_id": card_id,
        "study_id": f"doi:{doi}" if doi else f"url:{link}",
        "study_type": study_type,  # "systematic_review_or_meta_analysis" or "primary_study"
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
        "notes": note or "",
        "oa_pdf": pdf
    })

def build_card(cards_rows, studies_rows, seen, cfg_row):
    term = cfg_row["pubmed_query"]
    card_id = cfg_row["card_id"]

    # SR/MA from OpenAlex + PubMed (systematic subset + publication types)
    oa = openalex_reviews(cfg_row["openalex_query"], since=OPENALEX_SINCE)
    sr = pubmed_systematic_reviews(term, retmax=PUBMED_RETMAX)

    for r in oa:
        add_study_row(studies_rows, seen, card_id, "systematic_review_or_meta_analysis",
                      r["title"], r["year"], r["doi"], r["link"],
                      "SR/MA candidate (screen with PRISMA/AMSTAR-2).", EMAIL)
    for s in sr:
        add_study_row(studies_rows, seen, card_id, "systematic_review_or_meta_analysis",
                      s["title"], s["year"], s["doi"], s["link"],
                      "SR/MA from PubMed systematic subset / publication type.", EMAIL)

    # Primary studies (broad)
    pmids = pubmed_search(term, retmax=PUBMED_RETMAX)
    for s in pubmed_summary(pmids):
        add_study_row(studies_rows, seen, card_id, "primary_study",
                      s["title"], s["year"], s["doi"], s["link"],
                      "Candidate RCT/cohort; classify later (RoB 2 / ROBINS-I).", EMAIL)

    # Card row (now includes joint, procedure)
    cards_rows.append({
        "card_id": card_id,
        "joint": cfg_row.get("joint",""),
        "procedure": cfg_row.get("procedure",""),
        "entity_type": "feature",
        "entity_name": cfg_row["feature_name"],
        "outcome": cfg_row["outcome"],
        "comparator": cfg_row["comparator"],
        "bottom_line": cfg_row["bottom_line"],
        "certainty": cfg_row["certainty"],
        "registry_njr": cfg_row["registry_njr"],
        "registry_aoanjrr": cfg_row["registry_aoanjrr"],
        "registry_ajrr": cfg_row["registry_ajrr"],
        "last_updated": datetime.date.today().isoformat(),
        "tags": "shoulder;TSA"
    })

def main():
    cfg_path = "cards_config.csv"
    if not os.path.exists(cfg_path):
        print("Missing cards_config.csv in repo root."); sys.exit(1)

    cards_rows, studies_rows, seen = [], [], set()

    with open(cfg_path, newline="", encoding="utf-8") as cf:
        for row in csv.DictReader(cf):
            required = ["card_id","feature_name","pubmed_query"]
            if any(not row.get(k) for k in required):
                print(f"[warn] Skipping row (missing required fields): {row}"); continue
            build_card(cards_rows, studies_rows, seen, row)
            time.sleep(0.2)  # be polite

    if cards_rows:
        with open("evidence_cards.csv","w",newline="",encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(cards_rows[0].keys()))
            w.writeheader(); w.writerows(cards_rows)

    if studies_rows:
        fields = ["card_id","study_id","study_type","title","year","doi_or_link","design","n","follow_up_years",
                  "effect_measure","effect_value","ci_lower","ci_upper","funding","risk_of_bias_tool",
                  "risk_of_bias_overall","source_api","notes","oa_pdf"]
        with open("evidence_studies.csv","w",newline="",encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader(); w.writerows(studies_rows)

    print("Wrote evidence_cards.csv and evidence_studies.csv")

if __name__ == "__main__":
    main()
