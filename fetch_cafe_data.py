import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("GOOGLE_PLACES_API_KEY")

INPUT_FILE = "DOHMH_New_York_City_Restaurant_Inspection_Results.csv"
OUTPUT_FILE = "nyc_cafes_enriched.csv"

PLACES_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
PLACES_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"

# --------------------------------------------------------------------------
# Step 1: Load and deduplicate
# --------------------------------------------------------------------------
print("Loading data...")
df = pd.read_csv(INPUT_FILE)
df = df[df["CUISINE DESCRIPTION"] == "Coffee/Tea"]
df = df.dropna(subset=["Latitude", "Longitude"])
df = df[df["BORO"] != "0"]
df = df[df["BORO"] != 0]
df["INSPECTION DATE"] = pd.to_datetime(df["INSPECTION DATE"], errors="coerce")
df = df.sort_values("INSPECTION DATE", ascending=False)
df = df.drop_duplicates(subset="CAMIS", keep="first")
df = df.reset_index(drop=True)
print(f"Unique cafes after dedup: {len(df)}")


# --------------------------------------------------------------------------
# Step 3: API functions with retry
# --------------------------------------------------------------------------

def nearby_search(name, lat, lng):
    for attempt in range(3):
        try:
            params = {
                "location": f"{lat},{lng}",
                "radius": 50,
                "keyword": name,
                "type": "cafe",
                "key": API_KEY,
            }
            r = requests.get(PLACES_SEARCH_URL, params=params, timeout=15)
            data = r.json()

            if data.get("status") == "ZERO_RESULTS":
                params["radius"] = 150
                del params["type"]
                r = requests.get(PLACES_SEARCH_URL, params=params, timeout=15)
                data = r.json()

            results = data.get("results", [])
            return results[0]["place_id"] if results else None

        except requests.exceptions.Timeout:
            print(f"    Timeout on search (attempt {attempt+1}/3), retrying...")
            time.sleep(3)
        except Exception as e:
            print(f"    Error on search: {e}")
            return None
    return None


def get_details(place_id):
    for attempt in range(3):
        try:
            params = {
                "place_id": place_id,
                "fields": "name,user_ratings_total,price_level,opening_hours,types,rating",
                "key": API_KEY,
            }
            r = requests.get(PLACES_DETAILS_URL, params=params, timeout=15)
            result = r.json().get("result", {})

            hours = result.get("opening_hours", {})
            periods = hours.get("periods", [])
            latest_close = None
            if periods:
                close_times = [int(p["close"]["time"]) for p in periods if p.get("close", {}).get("time")]
                if close_times:
                    latest_close = max(close_times)

            types = result.get("types", [])

            return {
                "google_name": result.get("name"),
                "review_count": result.get("user_ratings_total"),
                "google_rating": result.get("rating"),
                "price_level": result.get("price_level"),
                "open_late": latest_close is not None and latest_close >= 1900,
                "types": "|".join(types),
            }

        except requests.exceptions.Timeout:
            print(f"    Timeout on details (attempt {attempt+1}/3), retrying...")
            time.sleep(3)
        except Exception as e:
            print(f"    Error on details: {e}")
            return None
    return None


# --------------------------------------------------------------------------
# Step 4: Main loop
# --------------------------------------------------------------------------

SPECIALTY_KEYWORDS = [
    "specialty", "roaster", "roastery", "brew bar", "third wave",
    "single origin", "espresso bar", "micro roast"
]

not_found = []
results = []
to_process = df

print(f"Cafes left to process: {len(to_process)}\n")

for i, row in to_process.iterrows():
    name = str(row["DBA"]).strip()
    lat = row["Latitude"]
    lng = row["Longitude"]
    boro = row["BORO"]
    zipcode = str(row["ZIPCODE"]).split(".")[0].zfill(5)
    building = str(row.get("BUILDING", "")).strip()
    street = str(row.get("STREET", "")).strip()
    camis = str(row["CAMIS"])

    global_index = len(results) + 1

    place_id = nearby_search(name, lat, lng)

    if not place_id:
        not_found.append({"CAMIS": camis, "DBA": name, "BORO": boro})
        record = {
            "CAMIS": camis, "DBA": name, "BORO": boro,
            "ZIPCODE": zipcode, "ADDRESS": f"{building} {street}".strip(),
            "Latitude": lat, "Longitude": lng,
            "google_name": None, "review_count": None, "google_rating": None,
            "price_level": None, "open_late": None, "types": None,
            "specialty_flag": None, "work_friendly_flag": None,
            "demand_tier": None, "pricing_power_flag": None,
        }
        results.append(record)
        print(f"  [{global_index}/{len(df)}] NOT FOUND: {name} ({boro})")
    else:
        details = get_details(place_id)

        if details is None:
            details = {
                "google_name": None, "review_count": None, "google_rating": None,
                "price_level": None, "open_late": None, "types": None,
            }

        name_lower = name.lower()
        types_lower = (details.get("types") or "").lower()
        specialty_flag = any(kw in name_lower or kw in types_lower for kw in SPECIALTY_KEYWORDS)
        work_friendly_flag = details.get("open_late") or False

        record = {
            "CAMIS": camis, "DBA": name, "BORO": boro,
            "ZIPCODE": zipcode, "ADDRESS": f"{building} {street}".strip(),
            "Latitude": lat, "Longitude": lng,
            **details,
            "specialty_flag": specialty_flag,
            "work_friendly_flag": work_friendly_flag,
            "demand_tier": None,
            "pricing_power_flag": None,
        }
        results.append(record)
        print(f"  [{global_index}/{len(df)}] {name} ({boro}) — reviews: {details.get('review_count')}, price: {details.get('price_level')}")

    time.sleep(0.22)



# --------------------------------------------------------------------------
# Step 5: Calculate demand tiers and pricing power
# --------------------------------------------------------------------------
out = pd.DataFrame(results)

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

out["pricing_power_flag"] = (
    (out["demand_tier"] == "High") &
    (out["price_level"] >= 3)
)

# --------------------------------------------------------------------------
# Step 6: Save final output
# --------------------------------------------------------------------------
out.to_csv(OUTPUT_FILE, index=False)

print(f"\nDone. Saved to {OUTPUT_FILE}")
print(f"Total cafes: {len(out)}")
print(f"Successfully matched: {out['review_count'].notna().sum()}")
print(f"Not found in Google Places: {len(not_found)}")

if not_found:
    pd.DataFrame(not_found).to_csv("not_found.csv", index=False)
    print("Not-found list saved to not_found.csv")