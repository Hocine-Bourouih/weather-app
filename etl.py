"""
Pipeline ETL — Weather Data
============================
Extract  : Appel API Open-Meteo (gratuite, sans clé)
Transform: Nettoyage avec pandas (types, valeurs manquantes)
Load     : Insertion dans une base SQLite
"""

import requests
import pandas as pd
import sqlite3
import time
from datetime import datetime, timezone

from config import BASE_URL, CITIES, DB_PATH


# ──────────────────────────────────────
# EXTRACT — Récupérer les données brutes
# ──────────────────────────────────────

def extract(cities: dict) -> list[dict]:
    """Appelle l'API Open-Meteo pour chaque ville.
    Gestion d'erreurs + retry (max 3 tentatives par ville).
    """
    raw_data = []

    for city_name, coords in cities.items():
        params = {
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "current": "temperature_2m,relative_humidity_2m,apparent_temperature,pressure_msl,wind_speed_10m,weather_code",
        }
        data = call_api_with_retry(BASE_URL, params, retries=3)

        if data:
            data["city_name"] = city_name  # On ajoute le nom de la ville
            raw_data.append(data)
            print(f"  [OK] {city_name}")
        else:
            print(f"  [ERREUR] {city_name} — impossible de récupérer les données")

    return raw_data


def call_api_with_retry(url: str, params: dict, retries: int = 3) -> dict | None:
    """Appel HTTP GET avec retry exponentiel en cas d'échec."""
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()  # Lève une exception si status != 200
            return response.json()
        except requests.exceptions.RequestException as e:
            wait = 2 ** attempt  # Backoff exponentiel : 1s, 2s, 4s
            print(f"    Tentative {attempt + 1}/{retries} échouée : {e}")
            if attempt < retries - 1:
                time.sleep(wait)

    return None


# Descriptions météo basées sur les codes WMO
WEATHER_DESCRIPTIONS = {
    0: "ciel dégagé", 1: "principalement dégagé", 2: "partiellement nuageux",
    3: "couvert", 45: "brouillard", 48: "brouillard givrant",
    51: "bruine légère", 53: "bruine modérée", 55: "bruine forte",
    61: "pluie légère", 63: "pluie modérée", 65: "pluie forte",
    71: "neige légère", 73: "neige modérée", 75: "neige forte",
    80: "averses légères", 81: "averses modérées", 82: "averses fortes",
    95: "orage", 96: "orage avec grêle légère", 99: "orage avec grêle forte",
}


# ──────────────────────────────────────
# TRANSFORM — Nettoyer et structurer
# ──────────────────────────────────────

def transform(raw_data: list[dict]) -> pd.DataFrame:
    """Transforme le JSON brut en DataFrame propre."""
    rows = []

    for entry in raw_data:
        current = entry.get("current", {})
        weather_code = current.get("weather_code")

        row = {
            "city": entry.get("city_name"),
            "temperature": current.get("temperature_2m"),
            "feels_like": current.get("apparent_temperature"),
            "humidity": current.get("relative_humidity_2m"),
            "pressure": current.get("pressure_msl"),
            "wind_speed": current.get("wind_speed_10m"),
            "description": WEATHER_DESCRIPTIONS.get(weather_code, "inconnu"),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        rows.append(row)

    df = pd.DataFrame(rows)

    # Nettoyage des types
    df["temperature"] = pd.to_numeric(df["temperature"], errors="coerce")
    df["feels_like"] = pd.to_numeric(df["feels_like"], errors="coerce")
    df["humidity"] = pd.to_numeric(df["humidity"], errors="coerce")
    df["pressure"] = pd.to_numeric(df["pressure"], errors="coerce")
    df["wind_speed"] = pd.to_numeric(df["wind_speed"], errors="coerce")

    # Suppression des lignes où la température est manquante (donnée essentielle)
    df = df.dropna(subset=["temperature"])

    return df


# ──────────────────────────────────────
# LOAD — Stocker dans SQLite
# ──────────────────────────────────────

def load(df: pd.DataFrame) -> None:
    """Insère le DataFrame dans la table SQLite."""
    conn = sqlite3.connect(DB_PATH)

    # Crée la table si elle n'existe pas
    conn.execute("""
        CREATE TABLE IF NOT EXISTS weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT,
            temperature REAL,
            feels_like REAL,
            humidity INTEGER,
            pressure INTEGER,
            wind_speed REAL,
            description TEXT,
            timestamp TEXT
        )
    """)

    # Insertion via pandas (simple et efficace)
    df.to_sql("weather", conn, if_exists="append", index=False)

    conn.commit()
    conn.close()

    print(f"  {len(df)} lignes insérées dans {DB_PATH}")


# ──────────────────────────────────────
# MAIN — Orchestration du pipeline
# ──────────────────────────────────────

def run():
    print(f"\n{'='*40}")
    print(f"Pipeline ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*40}")

    print("\n1. EXTRACT")
    raw_data = extract(CITIES)

    if not raw_data:
        print("Aucune donnée récupérée. Arrêt du pipeline.")
        return

    print("\n2. TRANSFORM")
    df = transform(raw_data)
    print(f"  {len(df)} lignes après nettoyage")
    print(df[["city", "temperature", "humidity"]].to_string(index=False))

    print("\n3. LOAD")
    load(df)

    print("\nPipeline terminé avec succès !\n")


if __name__ == "__main__":
    run()
