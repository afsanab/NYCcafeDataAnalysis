# NYC Cafe Demand Analysis

A data analysis project exploring what drives customer demand and pricing power for cafes in New York City. Built as part of a Data School application.

---

## Focus Question

**What drives customer demand and pricing power for cafes in New York City?**

---

## Hypotheses

1. Cafes associated with specialty or trend-driven offerings attract higher customer demand.
2. Cafes offering work-friendly amenities (longer hours, late closing) show higher sustained demand.
3. Location is a key driver of demand, but operational choices explain performance differences within the same neighborhood.

---

## Data Sources

- **NYC Open Data — DOHMH Restaurant Inspection Results**: provides a list of every licensed cafe in NYC with address, borough, ZIP code, and coordinates. Filtered to `Coffee/Tea` cuisine type.
- **Google Places API**: enriches each cafe with review count, price level, opening hours, and place types.

---

## Methodology

Customer demand is proxied by review count — the volume of customer engagement on Google. Pricing power is inferred by the ability of a cafe to sustain high demand at a higher price tier compared to nearby competitors.

**Metrics used:**
- Review count (demand proxy)
- Price level (Google's 1–4 scale)
- Open late flag (closes 7pm or later — work-friendly proxy)
- Specialty flag (name or place type contains specialty, roaster, brew bar, etc.)
- Demand tier (Low / Medium / High, split by tertiles)
- Pricing power flag (High demand + price level 3 or 4)

**Metrics excluded:**
- Star rating (biased, inflated, weak signal)
- Revenue (not available)
- Social media followers
- Review text sentiment (scope creep)

---

## Assumptions & Limitations

- Demand is proxied by review volume, not revenue or foot traffic
- Review data reflects engagement, not profitability
- Amenities are inferred from opening hours and place type tags, not verified directly
- Analysis is limited to cafes licensed and inspected by NYC DOHMH
- Google Places data reflects current state, not historical trends

---

## Dashboard

The Tableau dashboard is structured around five sections:

1. NYC cafe landscape — geographic overview by borough
2. H1: Specialty offerings and demand
3. H2: Amenities and sustained demand
4. H3: Within-neighborhood performance gaps
5. Takeaways and recommendations

---

## Setup

**Requirements:**
- Python 3.12+
- A Google Places API key with billing enabled (free tier covers this project)

**Install dependencies:**
```bash
python3 -m venv venv
source venv/bin/activate
pip install requests pandas python-dotenv
```

**Create a `.env` file in the project root:**
```
GOOGLE_PLACES_API_KEY=your_key_here
```

**Run the data fetch script:**
```bash
python3 fetch_cafe_data.py
```

This will produce `nyc_cafes_enriched.csv` — the cleaned, enriched dataset ready for Tableau.

---

## Project Structure

```
NYCcafeDataAnalysis/
├── data/
│   └── DOHMH_New_York_City_Restaurant_Inspection_Results.csv
├── fetch_cafe_data.py
├── NYCinspectionAnalytics.ipynb
├── .env                  # not committed
├── .gitignore
└── README.md
```