import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("GOOGLE_PLACES_API_KEY")

INPUT_FILE = "data/DOHMH_New_York_City_Restaurant_Inspection_Results_20260313.csv"
OUTPUT_FILE = "nyc_cafes_enriched.csv"

PLACES_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
PLACES_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"

# --------------------------------------------------------------------------
# Step 1: Load and deduplicate
# Keep most recent inspection per unique cafe (by CAMIS, which is a stable ID)
# --------------------------------------------------------------------------
print("Loading data...")
df = pd.read_csv(INPUT_FILE)

# Filter to Coffee/Tea only (should already be filtered, but just in case)
df = df[df["CUISINE DESCRIPTION"] == "Coffee/Tea"]

# Drop rows with no coordinates or bad borough
df = df.dropna(subset=["Latitude", "Longitude"])
df = df[df["BORO"] != "0"]
df = df[df["BORO"] != 0]

# Sort by inspection date descending, then deduplicate by CAMIS
df["INSPECTION DATE"] = pd.to_datetime(df["INSPECTION DATE"], errors="coerce")
df = df.sort_values("INSPECTION DATE", ascending=False)
df = df.drop_duplicates(subset="CAMIS", keep="first")

df = df.reset_index(drop=True)
print(f"Unique cafes after dedup: {len(df)}")

# --------------------------------------------------------------------------
# Step 2: For each cafe, query Google Places Nearby Search, then Details
# --------------------------------------------------------------------------

def nearby_search(name, lat, lng):
    """Find the best matching Place ID for a cafe near its coordinates."""
    params = {
        "location": f"{lat},{lng}",
        "radius": 50,          # 50 metres — tight radius, same building
        "keyword": name,
        "type": "cafe",
        "key": API_KEY,
    }
    r = requests.get(PLACES_SEARCH_URL, params=params, timeout=10)
    data = r.json()

    if data.get("status") == "ZERO_RESULTS":
        # Widen radius to 150m and drop type filter
        params["radius"] = 150
        del params["type"]
        r = requests.get(PLACES_SEARCH_URL, params=params, timeout=10)
        data = r.json()

    results = data.get("results", [])
    if not results:
        return None

    # Return the first result — tight radius makes this reliable
    return results[0]["place_id"]


def get_details(place_id):
    """Pull review count, price level, opening hours, and types."""
    params = {
        "place_id": place_id,
        "fields": "name,user_ratings_total,price_level,opening_hours,types,rating",
        "key": API_KEY,
    }
    r = requests.get(PLACES_DETAILS_URL, params=params, timeout=10)
    result = r.json().get("result", {})

    hours = result.get("opening_hours", {})
    periods = hours.get("periods", [])

    # Latest closing time across all days (to flag late-night cafes)
    latest_close = None
    if periods:
        close_times = []
        for p in periods:
            close = p.get("close", {}).get("time")
            if close:
                close_times.append(int(close))
        if close_times:
            latest_close = max(close_times)

    types = result.get("types", [])

    return {
        "google_name": result.get("name"),
        "review_count": result.get("user_ratings_total"),
        "google_rating": result.get("rating"),
        "price_level": result.get("price_level"),
        "open_late": latest_close is not None and latest_close >= 1900,  # closes 7pm or later
        "types": "|".join(types),
    }


# --------------------------------------------------------------------------
# Step 3: Run the loop with rate limiting and progress tracking
# --------------------------------------------------------------------------

results = []
not_found = []

SPECIALTY_KEYWORDS = [
    "specialty", "roaster", "roastery", "brew bar", "third wave",
    "single origin", "espresso bar", "micro roast"
]

print(f"\nFetching Google Places data for {len(df)} cafes...")
print("This will take a few minutes. Do not interrupt.\n")

for i, row in df.iterrows():
    name = str(row["DBA"]).strip()
    lat = row["Latitude"]
    lng = row["Longitude"]
    boro = row["BORO"]
    zipcode = str(row["ZIPCODE"]).split(".")[0].zfill(5)
    building = str(row.get("BUILDING", "")).strip()
    street = str(row.get("STREET", "")).strip()

    place_id = nearby_search(name, lat, lng)

    if not place_id:
        not_found.append({"CAMIS": row["CAMIS"], "DBA": name, "BORO": boro})
        results.append({
            "CAMIS": row["CAMIS"],
            "DBA": name,
            "BORO": boro,
            "ZIPCODE": zipcode,
            "ADDRESS": f"{building} {street}".strip(),
            "Latitude": lat,
            "Longitude": lng,
            "google_name": None,
            "review_count": None,
            "google_rating": None,
            "price_level": None,
            "open_late": None,
            "types": None,
            "specialty_flag": None,
            "work_friendly_flag": None,
            "demand_tier": None,
            "pricing_power_flag": None,
        })
        print(f"  [{i+1}/{len(df)}] NOT FOUND: {name} ({boro})")
        time.sleep(0.05)
        continue

    details = get_details(place_id)

    # Derived fields
    name_lower = name.lower()
    types_lower = (details["types"] or "").lower()
    specialty_flag = any(kw in name_lower or kw in types_lower for kw in SPECIALTY_KEYWORDS)
    work_friendly_flag = details["open_late"] or False

    results.append({
        "CAMIS": row["CAMIS"],
        "DBA": name,
        "BORO": boro,
        "ZIPCODE": zipcode,
        "ADDRESS": f"{building} {street}".strip(),
        "Latitude": lat,
        "Longitude": lng,
        **details,
        "specialty_flag": specialty_flag,
        "work_friendly_flag": work_friendly_flag,
        "demand_tier": None,         # filled below after all data is collected
        "pricing_power_flag": None,  # filled below
    })

    print(f"  [{i+1}/{len(df)}] {name} ({boro}) — reviews: {details['review_count']}, price: {details['price_level']}")

    # Google Places API rate limit: 10 requests/second
    # Two calls per cafe (search + details), so 0.22s pause is safe
    time.sleep(0.22)

# --------------------------------------------------------------------------
# Step 4: Calculate demand tiers and pricing power flag
# --------------------------------------------------------------------------

out = pd.DataFrame(results)

# Demand tier: split into thirds by review count (ignoring nulls)
review_counts = out["review_count"].dropna()
if len(review_counts) > 0:
    low_cut = review_counts.quantile(0.33)
    high_cut = review_counts.quantile(0.67)

    def demand_tier(rc):
        if pd.isna(rc):
            return None
        if rc <= low_cut:
            return "Low"
        elif rc <= high_cut:
            return "Medium"
        else:
            return "High"

    out["demand_tier"] = out["review_count"].apply(demand_tier)

# Pricing power flag: high demand AND price level 3 or 4
out["pricing_power_flag"] = (
    (out["demand_tier"] == "High") &
    (out["price_level"] >= 3)
)

# --------------------------------------------------------------------------
# Step 5: Save outputs
# --------------------------------------------------------------------------

out.to_csv(OUTPUT_FILE, index=False)
print(f"\nDone. Saved to {OUTPUT_FILE}")
print(f"Total cafes: {len(out)}")
print(f"Successfully matched: {out['review_count'].notna().sum()}")
print(f"Not found in Google Places: {len(not_found)}")

if not_found:
    pd.DataFrame(not_found).to_csv("not_found.csv", index=False)
    print("Not-found list saved to not_found.csv")

print("\nColumn summary:")
print(out[["DBA", "BORO", "review_count", "price_level", "open_late",
           "specialty_flag", "work_friendly_flag", "demand_tier",
           "pricing_power_flag"]].describe(include="all").to_string())