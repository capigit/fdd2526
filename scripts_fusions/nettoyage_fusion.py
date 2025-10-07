import sqlite3
import unicodedata

# === Connexion à la base fusionnée ===
db_path = "./bd/fusion_ieee.db"
conn = sqlite3.connect(db_path)
cur = conn.cursor()

def normalize_text(s):
    """Nettoie et normalise le texte (minuscules, sans accents, espaces trimés)."""
    if not s or not isinstance(s, str):
        return ""
    s = s.lower().strip()
    s = unicodedata.normalize('NFD', s).encode('ascii', 'ignore').decode('utf-8')
    return s

print("Nettoyage des articles...")

# 1. Normalisation des colonnes textuelles
cur.execute("SELECT id, title, publisher, published_in, doi FROM articles")
rows = cur.fetchall()

for row in rows:
    art_id, title, publisher, pub_in, doi = row
    title_n = normalize_text(title)
    publisher_n = normalize_text(publisher)
    pub_in_n = normalize_text(pub_in)
    doi_n = normalize_text(doi)
    cur.execute("""
        UPDATE articles
        SET title = ?, publisher = ?, published_in = ?, doi = ?
        WHERE id = ?
    """, (title_n, publisher_n, pub_in_n, doi_n, art_id))

conn.commit()

# 2. Suppression des doublons (même DOI ou même titre)
print("🧹 Suppression des doublons...")

cur.execute("""
DELETE FROM articles
WHERE id NOT IN (
    SELECT MIN(id)
    FROM articles
    GROUP BY doi
)
AND doi != ''
""")

# doublons sans DOI → basés sur le titre
cur.execute("""
DELETE FROM articles
WHERE id NOT IN (
    SELECT MIN(id)
    FROM articles
    WHERE doi = ''
    GROUP BY title
)
AND doi = ''
""")

conn.commit()

# 3. Nettoyage des mots-clés
print("Nettoyage des keywords...")
cur.execute("SELECT id, keyword FROM keywords")
rows = cur.fetchall()

for row in rows:
    kid, kw = row
    kw_norm = normalize_text(kw)
    cur.execute("UPDATE keywords SET keyword = ? WHERE id = ?", (kw_norm, kid))

conn.commit()

# 4. Nettoyage des auteurs
print("Nettoyage des auteurs...")
cur.execute("SELECT id, name FROM authors")
rows = cur.fetchall()

for row in rows:
    aid, name = row
    name_n = normalize_text(name)
    cur.execute("UPDATE authors SET name = ? WHERE id = ?", (name_n, aid))

conn.commit()

# 5. Suppression auteurs vides
cur.execute("DELETE FROM authors WHERE name = ''")
conn.commit()

print("\nNettoyage et dédoublonnage terminés !")
conn.close()