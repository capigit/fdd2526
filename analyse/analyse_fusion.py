import sqlite3
from collections import Counter

db_path = "./bd/fusion_ieee.db"
conn = sqlite3.connect(db_path)
cur = conn.cursor()

print("📊 Analyse de la base fusionnée\n")

# 1. Nombre total d'articles
cur.execute("SELECT COUNT(*) FROM articles")
nb_articles = cur.fetchone()[0]
print(f"📰 Nombre total d'articles : {nb_articles}")

# 2. Répartition par source
print("\n📂 Répartition par base d’origine :")
cur.execute("SELECT source, COUNT(*) FROM articles GROUP BY source")
for src, count in cur.fetchall():
    print(f"   - {src:30} {count} articles")

# 3. Nombre d’auteurs uniques
cur.execute("SELECT COUNT(DISTINCT name) FROM authors")
nb_authors = cur.fetchone()[0]
print(f"\n👥 Auteurs uniques : {nb_authors}")

# 4. Nombre de mots-clés uniques
cur.execute("SELECT COUNT(DISTINCT keyword) FROM keywords")
nb_keywords = cur.fetchone()[0]
print(f"🏷️  Mots-clés uniques : {nb_keywords}")

# 5. Top 10 mots-clés les plus fréquents
print("\n🔥 Top 10 mots-clés :")
cur.execute("""
    SELECT keyword, COUNT(*) as freq
    FROM keywords
    GROUP BY keyword
    ORDER BY freq DESC
    LIMIT 10
""")
for kw, freq in cur.fetchall():
    print(f"   - {kw:30} {freq}")

conn.close()
print("\n✅ Analyse terminée !")