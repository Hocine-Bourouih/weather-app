# Configuration du pipeline ETL météo
# ======================================
# API utilisée : Open-Meteo (gratuite, sans clé API)
# Documentation : https://open-meteo.com/

BASE_URL = "https://api.open-meteo.com/v1/forecast"

# Villes à surveiller (nom + coordonnées GPS)
CITIES = {
    "Paris":     {"lat": 48.86, "lon": 2.35},
    "Lyon":      {"lat": 45.76, "lon": 4.83},
    "Marseille": {"lat": 43.30, "lon": 5.37},
    "Toulouse":  {"lat": 43.60, "lon": 1.44},
    "Bordeaux":  {"lat": 44.84, "lon": -0.58},
}

# Base de données SQLite
DB_PATH = "weather.db"
