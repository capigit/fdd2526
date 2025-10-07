import json
import sqlite3
from pathlib import Path

# Charger le JSON
json_path = Path("IEEE_llm_Journals_1825.json")
if not json_path.exists():
    raise FileNotFoundError("Le fichier JSON est introuvable dans le dossier du projet.")

with open(json_path, "r", encoding="utf-8") as f:
    data = json.load(f)

# Connexion SQLite
db_path = Path("./bd/ieee_llm_articles.db")
conn = sqlite3.connect(db_path)
cur = conn.cursor()

# Création des tables
cur.executescript("""
CREATE TABLE IF NOT EXISTS articles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT,
    link TEXT,
    abstract TEXT,
    doi TEXT,
    date_of_publication TEXT,
    publisher TEXT,
    published_in TEXT,
    print_issn TEXT,
    electronic_issn TEXT
);

CREATE TABLE IF NOT EXISTS authors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    article_id INTEGER,
    name TEXT,
    lab TEXT,
    FOREIGN KEY(article_id) REFERENCES articles(id)
);

CREATE TABLE IF NOT EXISTS keywords (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    article_id INTEGER,
    type TEXT,
    keyword TEXT,
    FOREIGN KEY(article_id) REFERENCES articles(id)
);
""")

# Insertion des données
for item in data:
    details = item.get("Details", {})
    issn = item.get("issn_info", {})

    cur.execute("""
        INSERT INTO articles (title, link, abstract, doi, date_of_publication, publisher, published_in, print_issn, electronic_issn)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        item.get("Title"),
        item.get("Link"),
        item.get("Abstract"),
        details.get("DOI"),
        details.get("Date of Publication"),
        details.get("Publisher"),
        details.get("Published In"),
        issn.get("Print ISSN"),
        issn.get("Electronic ISSN")
    ))

    article_id = cur.lastrowid

    # Auteurs
    for author in item.get("authors_data", []):
        name = author.get("name")
        for lab in author.get("labs", []):
            cur.execute("INSERT INTO authors (article_id, name, lab) VALUES (?, ?, ?)", (article_id, name, lab))

    # Mots-clés
    keywords = item.get("keywords", {})
    for ktype, kwlist in keywords.items():
        for kw in kwlist:
            cur.execute("INSERT INTO keywords (article_id, type, keyword) VALUES (?, ?, ?)", (article_id, ktype, kw))

conn.commit()
conn.close()

print("Importation terminée : ieee_llm_articles.db créé avec succès !")