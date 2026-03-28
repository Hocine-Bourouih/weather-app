# Weather ETL Pipeline

Pipeline ETL simple qui collecte des données météo, les nettoie et les stocke dans SQLite.

## Stack

Python, requests, pandas, SQLite

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

1. Crée un compte sur [OpenWeatherMap](https://openweathermap.org/api) (gratuit)
2. Copie ta clé API dans `config.py`

## Utilisation

```bash
# Lancer le pipeline (Extract → Transform → Load)
python etl.py

# Consulter les données agrégées
python queries.py
```

## Scheduling avec cron

Pour exécuter le pipeline toutes les heures :

```bash
crontab -e
# Ajouter cette ligne :
0 * * * * cd /chemin/vers/weather-app && python etl.py >> etl.log 2>&1
```

## Structure

```
etl.py         — Pipeline principal (Extract, Transform, Load)
queries.py     — Requêtes SQL d'agrégation
config.py      — Configuration (API key, villes, BDD)
weather.db     — Base SQLite (créée automatiquement)
```
