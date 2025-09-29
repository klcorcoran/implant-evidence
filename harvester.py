# harvester.py — multi-card (stemless + metal-backed)
import csv, datetime, requests

EMAIL = "kam.l.corcoran@gmail.com"   # Unpaywall needs a real email

# ---- HTTP helper ---------------------------------------------------------
def safe_get(url, params=None, timeout=30):
    try:
        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r
    except Exception as e:
        print(f"[warn] GET failed: {url}  ({e})")
        return None

# ---- APIs ----------------------------------------------------------------
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

# ---- rows + builder ------------------------------------------------------
today = datetime.date.today().isoformat()
studies_rows, cards_rows, seen = [], [], set()

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

def build_card(feature_name, outcome, comparator, pubmed_query, openalex_query, card_id,
               bottom_line, certainty, registry_njr, registry_aoanjrr, registry_ajrr):
    # search & collect
    pmids = pubmed_search(pubmed_query, retmax=50)
    pm = pubmed_summary(pmids)
    oa = openalex_reviews(openalex_query, since="2018-01-01")

    for r in oa:
        add_row(card_id, "systematic_review_or_meta_analysis", r["title"], r["year"], r["doi"], r["link"],
                "SR/MA candidate (PRISMA/AMSTAR-2 to assess).")
    for s in pm:
        add_row(card_id, "primary_study", s["title"], s["year"], s["doi"], s["link"],
                "Candidate RCT/cohort; classify later (RoB 2 / ROBINS-I).")

    cards_rows.append({
        "card_id": card_id,
        "entity_type": "feature",
        "entity_name": feature_name,
        "outcome": outcome,
        "comparator": comparator,
        "bottom_line": bottom_line,
        "certainty": certainty,
        "registry_njr": registry_njr,
        "registry_aoanjrr": registry_aoanjrr,
        "registry_ajrr": registry_ajrr,
        "last_updated": today,
        "tags": "shoulder;TSA"
    })

# ---- CARD 1: Stemless humeral component (existing) -----------------------
build_card(
    feature_name="Stemless humeral component (aTSA)",
    outcome="5yr_all_cause_revision",
    comparator="Stemmed aTSA",
    pubmed_query='(stemless[Title/Abstract]) AND ("total shoulder arthroplasty"[Title/Abstract]) AND (revision OR survivorship)',
    openalex_query='stemless anatomic total shoulder arthroplasty revision',
    card_id="card_stemless_any_5yr",
    bottom_line="Across SR/MA, one RCT, and adjusted registry analyses, stemless shows ~comparable 5-yr revision vs stemmed; signals confounded when metal-backed glenoids are included.",
    certainty="moderate",
    registry_njr="limited shoulder coverage",
    registry_aoanjrr="Comparable stemless vs stemmed after excluding metal-backed glenoids.",
    registry_ajrr="Survivorship broadly comparable; see Annual Report."
)

# ---- CARD 2: Metal-backed glenoid (aTSA) ---------------------------------
build_card(
    feature_name="Metal-backed glenoid (aTSA)",
    outcome="5yr_all_cause_revision",
    comparator="Cemented all-polyethylene glenoid (aTSA)",
    pubmed_query='(("metal-backed"[Title/Abstract]) OR ("metal backed"[Title/Abstract]) OR ("trabecular metal"[Title/Abstract])) AND (("total shoulder arthroplasty"[Title/Abstract]) OR ("anatomic shoulder arthroplasty"[Title/Abstract])) AND (revision OR survivorship OR failure) NOT (reverse[Title/Abstract])',
    openalex_query='metal-backed glenoid anatomic total shoulder arthroplasty revision',
    card_id="card_metalbacked_any_5yr",
    bottom_line="SR/MA and registries show higher revision/failure with metal-backed glenoids vs cemented all-poly in aTSA; mid-term RCTs of modern MBG show no clear superiority. Prefer cemented all-poly for routine aTSA; use MBG with caution.",
    certainty="moderate",
    registry_njr="limited shoulder reporting",
    registry_aoanjrr="Registry collaboration shows higher revision when MBG used; excluding MBG equalizes stemless vs stemmed.",
    registry_ajrr="Narrative caution around MBG in annual reports; confirm per latest edition."
)

# ---- CARD 3: Cemented all-polyethylene glenoid (aTSA) --------------------
build_card(
    feature_name="Cemented all-polyethylene glenoid (aTSA)",
    outcome="5yr_all_cause_revision",
    comparator="Metal-backed glenoid (aTSA)",
    pubmed_query=(
        '(("all polyethylene"[Title/Abstract]) OR ("all-polyethylene"[Title/Abstract]) '
        'OR ("polyethylene glenoid"[Title/Abstract])) AND '
        '(("total shoulder arthroplasty"[Title/Abstract]) OR ("anatomic shoulder arthroplasty"[Title/Abstract])) '
        'AND (revision OR survivorship OR failure) NOT (reverse[Title/Abstract])'
    ),
    openalex_query='all polyethylene glenoid anatomic total shoulder arthroplasty revision',
    card_id="card_allpoly_any_5yr",
    bottom_line=(
        "Cemented all-poly glenoids remain the reference option for aTSA with favorable mid-term survivorship. "
        "Multiple SR/registry analyses report higher revision/failure with metal-backed glenoids; "
        "modern TM-backed RCTs show no superiority at 5 years. Prefer cemented all-poly for routine aTSA."
    ),
    certainty="moderate",
    registry_njr="limited shoulder reporting",
    registry_aoanjrr="Registry collaboration indicates higher revision when metal-backed glenoids are used; all-poly performs best.",
    registry_ajrr="Annual report narratives caution on MBG; all-poly cemented widely used."
)

# ---- write CSVs ----------------------------------------------------------
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
