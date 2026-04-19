"""
Fetches PVWatts v8 simulation results for California cities via NREL API.
Saves each city's JSON response to weather_cache/{name}_pvwatts.json.

Uses the PVWatts v8 endpoint (lat/lon → simulation outputs) instead of the
NSRDB TMY CSV download endpoint.  Rate limit: 1,000 req/hr → 4s between calls.
"""

import json
import os
import time
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env.local")

API_KEY  = os.getenv("NREL_API_KEY")
CACHE_DIR = Path(__file__).parent / "weather_cache"
PVWATTS_URL = "https://developer.nrel.gov/api/pvwatts/v8.json"

# Representative 10 kW south-facing roof-mounted system used for per-city
# baseline validation.  Per-permit simulation in Databricks uses each permit's
# actual system_size_kw from ZenPower data.
SYSTEM_DEFAULTS = {
    "system_capacity": 10,
    "module_type":     0,      # standard
    "losses":          14,
    "array_type":      1,      # fixed roof-mounted
    "azimuth":         180,    # south-facing
    "dataset":         "nsrdb",
    "timeframe":       "monthly",
}

CALIFORNIA_CITIES = [
    {"name": "Alameda",          "lat": 37.7652, "lon": -122.2416},
    {"name": "Anaheim",          "lat": 33.8366, "lon": -117.9143},
    {"name": "Antioch",          "lat": 37.9963, "lon": -121.8058},
    {"name": "Bakersfield",      "lat": 35.3733, "lon": -119.0187},
    {"name": "Berkeley",         "lat": 37.8716, "lon": -122.2727},
    {"name": "Burbank",          "lat": 34.1808, "lon": -118.3090},
    {"name": "Carlsbad",         "lat": 33.1581, "lon": -117.3506},
    {"name": "Chico",            "lat": 39.7285, "lon": -121.8375},
    {"name": "Chula_Vista",      "lat": 32.6401, "lon": -117.0842},
    {"name": "Clovis",           "lat": 36.8252, "lon": -119.7029},
    {"name": "Compton",          "lat": 33.8958, "lon": -118.2201},
    {"name": "Concord",          "lat": 37.9779, "lon": -122.0311},
    {"name": "Corona",           "lat": 33.8753, "lon": -117.5664},
    {"name": "Costa_Mesa",       "lat": 33.6411, "lon": -117.9187},
    {"name": "Daly_City",        "lat": 37.6879, "lon": -122.4702},
    {"name": "Davis",            "lat": 38.5449, "lon": -121.7405},
    {"name": "El_Cajon",         "lat": 32.7948, "lon": -116.9625},
    {"name": "El_Monte",         "lat": 34.0686, "lon": -118.0276},
    {"name": "Elk_Grove",        "lat": 38.4088, "lon": -121.3716},
    {"name": "Escondido",        "lat": 33.1192, "lon": -117.0864},
    {"name": "Eureka",           "lat": 40.8021, "lon": -124.1637},
    {"name": "Fairfield",        "lat": 38.2494, "lon": -122.0400},
    {"name": "Fontana",          "lat": 34.0922, "lon": -117.4350},
    {"name": "Fremont",          "lat": 37.5485, "lon": -121.9886},
    {"name": "Fresno",           "lat": 36.7378, "lon": -119.7871},
    {"name": "Fullerton",        "lat": 33.8704, "lon": -117.9242},
    {"name": "Garden_Grove",     "lat": 33.7743, "lon": -117.9378},
    {"name": "Glendale",         "lat": 34.1425, "lon": -118.2551},
    {"name": "Hayward",          "lat": 37.6688, "lon": -122.0808},
    {"name": "Huntington_Beach", "lat": 33.6595, "lon": -117.9988},
    {"name": "Irvine",           "lat": 33.6846, "lon": -117.8265},
    {"name": "Lancaster",        "lat": 34.6868, "lon": -118.1542},
    {"name": "Long_Beach",       "lat": 33.7701, "lon": -118.1937},
    {"name": "Los_Angeles",      "lat": 34.0522, "lon": -118.2437},
    {"name": "Modesto",          "lat": 37.6391, "lon": -120.9969},
    {"name": "Moreno_Valley",    "lat": 33.9425, "lon": -117.2297},
    {"name": "Murrieta",         "lat": 33.5539, "lon": -117.2139},
    {"name": "Newport_Beach",    "lat": 33.6189, "lon": -117.9289},
    {"name": "Norwalk",          "lat": 33.9022, "lon": -118.0817},
    {"name": "Oakland",          "lat": 37.8044, "lon": -122.2711},
    {"name": "Oceanside",        "lat": 33.1959, "lon": -117.3795},
    {"name": "Ontario",          "lat": 34.0633, "lon": -117.6509},
    {"name": "Orange",           "lat": 33.7879, "lon": -117.8531},
    {"name": "Oxnard",           "lat": 34.1975, "lon": -119.1771},
    {"name": "Palm_Springs",     "lat": 33.8303, "lon": -116.5453},
    {"name": "Palmdale",         "lat": 34.5794, "lon": -118.1165},
    {"name": "Pasadena",         "lat": 34.1478, "lon": -118.1445},
    {"name": "Pomona",           "lat": 34.0551, "lon": -117.7500},
    {"name": "Rancho_Cucamonga", "lat": 34.1064, "lon": -117.5931},
    {"name": "Redding",          "lat": 40.5865, "lon": -122.3917},
    {"name": "Rialto",           "lat": 34.1064, "lon": -117.3703},
    {"name": "Richmond",         "lat": 37.9358, "lon": -122.3477},
    {"name": "Riverside",        "lat": 33.9806, "lon": -117.3755},
    {"name": "Roseville",        "lat": 38.7521, "lon": -121.2880},
    {"name": "Sacramento",       "lat": 38.5816, "lon": -121.4944},
    {"name": "Salinas",          "lat": 36.6777, "lon": -121.6555},
    {"name": "San_Bernardino",   "lat": 34.1083, "lon": -117.2898},
    {"name": "San_Diego",        "lat": 32.7157, "lon": -117.1611},
    {"name": "San_Francisco",    "lat": 37.7749, "lon": -122.4194},
    {"name": "San_Jose",         "lat": 37.3382, "lon": -121.8863},
    {"name": "San_Mateo",        "lat": 37.5630, "lon": -122.3255},
    {"name": "Santa_Ana",        "lat": 33.7455, "lon": -117.8677},
    {"name": "Santa_Barbara",    "lat": 34.4208, "lon": -119.6982},
    {"name": "Santa_Clara",      "lat": 37.3541, "lon": -121.9552},
    {"name": "Santa_Clarita",    "lat": 34.3917, "lon": -118.5426},
    {"name": "Santa_Rosa",       "lat": 38.4404, "lon": -122.7141},
    {"name": "Simi_Valley",      "lat": 34.2694, "lon": -118.7815},
    {"name": "South_Gate",       "lat": 33.9545, "lon": -118.2120},
    {"name": "Stockton",         "lat": 37.9577, "lon": -121.2908},
    {"name": "Sunnyvale",        "lat": 37.3688, "lon": -122.0363},
    {"name": "Thousand_Oaks",    "lat": 34.1706, "lon": -118.8376},
    {"name": "Torrance",         "lat": 33.8358, "lon": -118.3406},
    {"name": "Vallejo",          "lat": 38.1041, "lon": -122.2566},
    {"name": "Victorville",      "lat": 34.5362, "lon": -117.2928},
    {"name": "Visalia",          "lat": 36.3302, "lon": -119.2921},
    {"name": "West_Covina",      "lat": 34.0686, "lon": -117.9390},
]


# ── Global backoff state ──────────────────────────────────────────────────────
# PVWatts allows 1,000 req/hr. A 429 blocks the entire key, so one rate-limit
# hit must pause ALL subsequent requests, not just the city that triggered it.
_resume_at: float = 0.0


def _backoff(seconds: int, reason: str) -> None:
    global _resume_at
    new_resume = time.monotonic() + seconds
    if new_resume > _resume_at:
        _resume_at = new_resume
    print(f"  [backoff] {reason} — pausing all requests for {seconds}s")


def _wait() -> None:
    remaining = _resume_at - time.monotonic()
    if remaining > 0:
        print(f"  [wait]   {remaining:.0f}s global cooldown remaining…")
        time.sleep(remaining)
    time.sleep(4.0)  # 1,000 req/hr → 1 per 3.6s; 4s keeps safely under


def simulate_city(city: dict) -> bool:
    out_path = CACHE_DIR / f"{city['name']}_pvwatts.json"
    if out_path.exists():
        print(f"  [skip] {city['name']} already cached")
        return True

    params = {
        "api_key": API_KEY,
        "lat":     city["lat"],
        "lon":     city["lon"],
        "tilt":    round(abs(city["lat"])),  # latitude tilt is the optimal rule of thumb
        **SYSTEM_DEFAULTS,
    }

    for attempt in range(1, 4):
        _wait()
        resp = requests.get(PVWATTS_URL, params=params, timeout=30)

        if resp.status_code == 200:
            data = resp.json()
            if data.get("errors"):
                print(f"  [err]  {city['name']}: API errors — {data['errors']}")
                return False
            out_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
            outputs = data["outputs"]
            print(
                f"  [ok]   {city['name']}: "
                f"{outputs['ac_annual']:.0f} kWh/yr  "
                f"CF={outputs['capacity_factor']:.1f}%"
            )
            return True

        if resp.status_code == 429:
            wait = 120 * attempt  # 120s → 240s → 360s
            _backoff(wait, f"429 on {city['name']} (attempt {attempt}/3)")
            continue

        # Any other non-200 is not retryable
        print(f"  [err]  {city['name']}: HTTP {resp.status_code} — {resp.text[:300]}")
        return False

    print(f"  [fail] {city['name']}: gave up after 3 attempts")
    return False


def main():
    if not API_KEY:
        raise EnvironmentError("NREL_API_KEY not set — add it to .env.local")

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Simulating PVWatts for {len(CALIFORNIA_CITIES)} California cities...\n")

    ok = err = 0
    for city in CALIFORNIA_CITIES:
        if simulate_city(city):
            ok += 1
        else:
            err += 1

    print(f"\nDone — {ok} simulated, {err} failed.")


if __name__ == "__main__":
    main()
