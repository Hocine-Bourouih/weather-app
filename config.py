# Configuration du pipeline ETL météo
# ======================================
# API utilisée : OpenWeatherMap (gratuite)
# Inscription : https://openweathermap.org/api → récupère ta clé API

API_KEY = "YOUR_API_KEY"  # Remplace par ta clé OpenWeatherMap
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

# Liste des villes à surveiller
CITIES = ["Paris", "Lyon", "Marseille", "Toulouse", "Bordeaux"]

# Base de données SQLite
DB_PATH = "weather.db"
