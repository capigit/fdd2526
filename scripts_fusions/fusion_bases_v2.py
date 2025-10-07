import sqlite3
from pathlib import Path
import os

base_dir = Path("bd")
fusion_path = base_dir / "fusion_ieee.db"

if fusion_path.exists():
    os.remove(fusion_path)

fusion_conn = sqlite3.connect(fusion_path)
fusion_cur = fusion_conn.cursor()

# === Schéma harmonisé ===
fusion_cur.executescript("""
CREATE TABLE articles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT,
    link TEXT,
    abstract TEXT,
    doi TEXT,
    date_publication TEXT,
    publisher TEXT,
    published_in TEXT,
    print_issn TEXT,
    electronic_issn TEXT,
    source TEXT
);

CREATE TABLE authors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    source TEXT
);

CREATE TABLE labs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lab_name TEXT,
    source TEXT
);

CREATE TABLE article_authors (
    article_id INTEGER,
    author_id INTEGER
);

CREATE TABLE author_labs (
    author_id INTEGER,
    lab_id INTEGER
);

CREATE TABLE keywords (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    article_id INTEGER,
    type TEXT,
    keyword TEXT,
    source TEXT
);
""")

fusion_conn.commit()

bases = [
    "ieee_ai_articles.db",
    "ieee_llm_articles.db",
    "ieee_deep_learning.db",
    "ieee_nlp.db",
    "ieee_machine_learning.db"
]


def normalize_date_columns(article):
    if "date_of_publication" in article.keys():
        article["date_publication"] = article.pop("date_of_publication")
    return article


def insert_with_source(table, data, source):
    # On ignore 'id' pour laisser SQLite auto-générer
    data.pop("id", None)
    cols = ", ".join(data.keys()) + ", source"
    placeholders = ", ".join(["?"] * (len(data) + 1))
    values = list(data.values()) + [source]
    fusion_cur.execute(f"INSERT INTO {table} ({cols}) VALUES ({placeholders})", values)


for base_name in bases:
    db_path = base_dir / base_name
    if not db_path.exists():
        print(f"⚠️ Base manquante : {base_name}")
        continue

    print(f"\n🔹 Fusion de {base_name}...")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # ARTICLES
    try:
        for row in cur.execute("SELECT * FROM articles"):
            data = dict(row)
            data = normalize_date_columns(data)
            insert_with_source("articles", data, base_name)
    except Exception as e:
        print(f"Erreur articles {base_name}: {e}")

    # AUTHORS
    try:
        cur.execute("PRAGMA table_info(authors)")
        cols = [c[1] for c in cur.fetchall()]
        if "name" in cols:
            for row in cur.execute("SELECT name FROM authors"):
                insert_with_source("authors", dict(row), base_name)
    except Exception as e:
        print(f"Erreur authors {base_name}: {e}")

    # LABS
    try:
        cur.execute("SELECT lab_name FROM labs")
        for row in cur.fetchall():
            insert_with_source("labs", dict(row), base_name)
    except:
        pass

    # KEYWORDS
    try:
        for row in cur.execute("SELECT * FROM keywords"):
            data = dict(row)
            insert_with_source("keywords", data, base_name)
    except Exception as e:
        print(f"Erreur keywords {base_name}: {e}")

    conn.close()
    fusion_conn.commit()

print("\n✅ Fusion terminée sans erreur de clé unique !")
fusion_conn.close()