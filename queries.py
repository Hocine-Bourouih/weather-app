"""
Requêtes d'agrégation SQL
==========================
Script pour explorer les données collectées par le pipeline ETL.
"""

import sqlite3
from config import DB_PATH


def run_query(title: str, sql: str):
    """Exécute une requête SQL et affiche le résultat."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.execute(sql)
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    conn.close()

    print(f"\n--- {title} ---")
    # Affichage simple en colonnes
    print("  |  ".join(columns))
    print("-" * 60)
    for row in rows:
        print("  |  ".join(str(v) for v in row))


def main():
    # 1. Température moyenne par ville
    run_query(
        "Température moyenne par ville",
        """
        SELECT city,
               ROUND(AVG(temperature), 1) AS temp_moy,
               ROUND(MIN(temperature), 1) AS temp_min,
               ROUND(MAX(temperature), 1) AS temp_max,
               COUNT(*) AS nb_mesures
        FROM weather
        GROUP BY city
        ORDER BY temp_moy DESC
        """
    )

    # 2. Dernière mesure par ville
    run_query(
        "Dernière mesure par ville",
        """
        SELECT city, temperature, humidity, description, timestamp
        FROM weather w1
        WHERE timestamp = (
            SELECT MAX(timestamp) FROM weather w2 WHERE w2.city = w1.city
        )
        ORDER BY city
        """
    )

    # 3. Jours les plus chauds (top 5)
    run_query(
        "Top 5 des relevés les plus chauds",
        """
        SELECT city, temperature, timestamp
        FROM weather
        ORDER BY temperature DESC
        LIMIT 5
        """
    )

    # 4. Humidité moyenne par ville
    run_query(
        "Humidité moyenne par ville",
        """
        SELECT city,
               ROUND(AVG(humidity), 0) AS humidite_moy
        FROM weather
        GROUP BY city
        ORDER BY humidite_moy DESC
        """
    )


if __name__ == "__main__":
    main()
