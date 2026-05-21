"""
FlightRadar24 Scraper
=====================
Wymaga instalacji: pip install FlightRadarAPI curl_cffi

Dokumentacja biblioteki: https://github.com/JeanExtreme002/FlightRadarAPI
"""

from FlightRadar24 import FlightRadar24API
import json
import csv
import time
from datetime import datetime


def get_flights_in_area(lat1, lon1, lat2, lon2):
    """Pobiera loty w danym obszarze geograficznym."""
    api = FlightRadar24API()
    bounds = api.get_bounds_by_point(lat1, lon1, lat2, lon2)
    flights = api.get_flights(bounds=f"{lat1},{lat2},{lon1},{lon2}")
    return flights


def get_flights_over_poland():
    """Pobiera wszystkie loty aktualnie nad Polską."""
    api = FlightRadar24API()
    bounds = "55.0,49.0,14.0,24.0"
    flights = api.get_flights(bounds=bounds)
    
    results = []
    for flight in flights:
        results.append({
            "id":             flight.id,
            "callsign":       flight.callsign,
            "airline":        flight.airline_icao,
            "aircraft":       flight.aircraft_code,
            "origin":         flight.origin_airport_iata,
            "destination":    flight.destination_airport_iata,
            "latitude":       flight.latitude,
            "longitude":      flight.longitude,
            "altitude_ft":    flight.altitude,
            "speed_kt":       flight.ground_speed,
            "heading":        flight.heading,
            "vertical_speed": flight.vertical_speed,
            "squawk":         flight.squawk,
            "timestamp":      datetime.utcnow().isoformat(),
        })
    return results


def get_flight_details(flight_id: str):
    """Pobiera szczegółowe informacje o konkretnym locie."""
    api = FlightRadar24API()
    flights = api.get_flights(airline="RYR")
    
    for flight in flights:
        if flight.callsign == flight_id or flight.id == flight_id:
            return api.get_flight_details(flight)
    return None


def get_airline_flights(airline_icao: str):
    """Pobiera loty konkretnej linii lotniczej."""
    api = FlightRadar24API()
    flights = api.get_flights(airline=airline_icao)
    
    results = []
    for flight in flights:
        results.append({
            "callsign":    flight.callsign,
            "aircraft":    flight.aircraft_code,
            "origin":      flight.origin_airport_iata,
            "destination": flight.destination_airport_iata,
            "altitude_ft": flight.altitude,
            "speed_kt":    flight.ground_speed,
            "lat":         flight.latitude,
            "lon":         flight.longitude,
        })
    return results


def get_airport_arrivals_departures(airport_iata: str, limit: int = 25, mode: str = "current"):
    """
    Pobiera przyloty i odloty z lotniska.
    
    Parametr `mode`:
      - "current": pobiera bieżące i nadchodzące loty (domyślnie)
      - "earlier": działa jak przycisk 'Earlier flights' na FR24 (pobiera loty z przeszłości)
    """
    try:
        from curl_cffi import requests
    except ImportError:
        print("[!] Brakuje biblioteki. Zainstaluj: pip install curl_cffi")
        return {"airport": airport_iata, "arrivals": [], "departures": []}

    HEADERS = {
        "Accept": "application/json",
        "Referer": "https://www.flightradar24.com/",
    }

    url = "https://api.flightradar24.com/common/v1/airport.json"
    
    # Podstawowe parametry
    params = {
        "code": airport_iata,
        "plugin[]": "schedule",
        "limit": limit,
        "page": 1,
        "format": "json",
    }

    # Magia przycisku "Earlier flights"
    if mode == "earlier":
        params["page"] = -1
        params["timestamp"] = int(time.time())  # Ustawiamy punkt odniesienia na "teraz" i cofamy się (-1)

    try:
        resp = requests.get(url, params=params, headers=HEADERS, impersonate="chrome", timeout=15)
        resp.raise_for_status()
    except Exception as e:
        print(f"\n[!] Błąd pobierania danych z lotniska: {e}")
        return {"airport": airport_iata, "arrivals": [], "departures": []}

    data = resp.json()

    schedule = (data.get("result", {})
                    .get("response", {})
                    .get("airport", {})
                    .get("pluginData", {})
                    .get("schedule", {}))

    arrivals_raw   = schedule.get("arrivals",   {}).get("data", [])
    departures_raw = schedule.get("departures", {}).get("data", [])

    def format_ts(ts):
        """Konwertuje timestamp na godzinę (HH:MM)."""
        if ts:
            return datetime.fromtimestamp(ts).strftime('%H:%M')
        return "Brak"

    def parse_flight(f):
        fi      = f.get("flight", {})
        ident   = fi.get("identification", {})
        airline = fi.get("airline") or {}
        orig    = (fi.get("airport", {}).get("origin")       or {})
        dest    = (fi.get("airport", {}).get("destination")  or {})
        status  = (fi.get("status") or {})
        times   = (fi.get("time")   or {})
        
        scheduled = times.get("scheduled") or {}
        
        return {
            "flight_number":       ident.get("number", {}).get("default"),
            "callsign":            ident.get("callsign"),
            "airline":             airline.get("name") or airline.get("short") or "",
            "origin_iata":         (orig.get("code") or {}).get("iata"),
            "origin_city":         (orig.get("position", {}) or {}).get("region", {}).get("city"),
            "dest_iata":           (dest.get("code") or {}).get("iata"),
            "dest_city":           (dest.get("position", {}) or {}).get("region", {}).get("city"),
            "status":              status.get("text"),
            "scheduled_departure": format_ts(scheduled.get("departure")),
            "scheduled_arrival":   format_ts(scheduled.get("arrival"))
        }

    return {
        "airport":    airport_iata,
        "mode":       mode,
        "arrivals":   [parse_flight(f) for f in arrivals_raw],
        "departures": [parse_flight(f) for f in departures_raw],
    }


def save_to_csv(flights: list, filename: str = "flights.csv"):
    """Zapisuje listę lotów do pliku CSV."""
    if not flights:
        return
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=flights[0].keys())
        writer.writeheader()
        writer.writerows(flights)
    print(f"Zapisano {len(flights)} rekordów → {filename}")


# ─────────────────────────────────────────────
# PRZYKŁADY UŻYCIA
# ─────────────────────────────────────────────
if __name__ == "__main__":
    
    # 1. Bieżące i nadchodzące odloty/przyloty z Warszawy
    print("\n=== WAW - BIEŻĄCE LOTY ===")
    waw_current = get_airport_arrivals_departures("WAW", limit=5, mode="current")
    
    print("  Przyloty (Arrivals):")
    for f in waw_current["arrivals"]:
        print(f"    Planowy przylot: {f['scheduled_arrival']:5} | {str(f['flight_number']):8} z: {f['origin_iata']:3}  [{f['status']}]")
        
    print("\n  Odloty (Departures):")
    for f in waw_current["departures"]:
        print(f"    Wylot: {f['scheduled_departure']:5} → Przylot: {f['scheduled_arrival']:5} | {str(f['flight_number']):8} do: {f['dest_iata']:3}  [{f['status']}]")

    
    # 2. Wcześniejsze loty (symulacja przycisku 'Earlier flights')
    print("\n=== WAW - WCZEŚNIEJSZE LOTY (EARLIER FLIGHTS) ===")
    waw_earlier = get_airport_arrivals_departures("WAW", limit=30, mode="earlier")
    
    print("  Przyloty (Arrivals):")
    for f in waw_earlier["arrivals"]:
        print(f"    Planowy przylot: {f['scheduled_arrival']:5} | {str(f['flight_number']):8} z: {f['origin_iata']:3}  [{f['status']}]")
        
    print("\n  Odloty (Departures):")
    for f in waw_earlier["departures"]:
        print(f"    Wylot: {f['scheduled_departure']:5} → Przylot: {f['scheduled_arrival']:5} | {str(f['flight_number']):8} do: {f['dest_iata']:3}  [{f['status']}]")
