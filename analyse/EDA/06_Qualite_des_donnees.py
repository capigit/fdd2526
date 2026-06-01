"""
Analyse de la qualité des données
- Articles sans auteurs
- Articles sans mots-clés
- Labs sans pays
- Nettoyage final si nécessaire
"""

import sqlite3
import pandas as pd

db_path = "bd/fusion_ieee.db"
conn = sqlite3.connect(db_path)

# Articles sans auteurs
query_articles_sans_auth = """
SELECT a.id, a.title
FROM articles a
LEFT JOIN article_authors aa ON a.id = aa.article_id
WHERE aa.author_id IS NULL
"""
df_articles_sans_auth = pd.read_sql(query_articles_sans_auth, conn)
print("Articles sans auteurs :", len(df_articles_sans_auth))

# Articles sans mots-clés
query_articles_sans_kw = """
SELECT a.id, a.title
FROM articles a
LEFT JOIN keywords k ON a.id = k.article_id
WHERE k.keyword IS NULL
"""
df_articles_sans_kw = pd.read_sql(query_articles_sans_kw, conn)
print("Articles sans mots-clés :", len(df_articles_sans_kw))

# Labs sans pays
query_labs_sans_pays = """
SELECT id, lab_name
FROM labs
WHERE country IS NULL OR country=''
"""
df_labs_sans_pays = pd.read_sql(query_labs_sans_pays, conn)
print("Labs sans pays :", len(df_labs_sans_pays))

conn.close()