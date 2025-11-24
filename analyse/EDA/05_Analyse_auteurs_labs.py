"""
Analyse des auteurs et laboratoires
- Top 10 auteurs les plus productifs
- Top laboratoires
- Répartition géographique
"""

import sqlite3
import pandas as pd

db_path = "bd/fusion_ieee.db"
conn = sqlite3.connect(db_path)

# Articles par auteur
query_auth = """
SELECT aa.author_id, a.name, COUNT(aa.article_id) AS nb_articles
FROM article_authors aa
JOIN authors a ON aa.author_id = a.id
GROUP BY aa.author_id, a.name
ORDER BY nb_articles DESC
"""
df_authors = pd.read_sql(query_auth, conn)
print("Top 10 auteurs les plus productifs :")
print(df_authors.head(10))

# Laboratoires les plus actifs
query_labs = """
SELECT l.id, l.lab_name, l.country, COUNT(al.author_id) AS nb_auteurs
FROM author_labs al
JOIN labs l ON al.lab_id = l.id
GROUP BY l.id, l.lab_name, l.country
ORDER BY nb_auteurs DESC
"""
df_labs = pd.read_sql(query_labs, conn)
print("\nTop 10 laboratoires les plus actifs :")
print(df_labs.head(10))

# Répartition géographique des labs
df_country = df_labs.groupby('country')['nb_auteurs'].sum().sort_values(ascending=False)
print("\nRépartition géographique des laboratoires :")
print(df_country)

conn.close()