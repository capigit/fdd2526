import json
import sqlite3
from pathlib import Path

# chemins
base_dir = Path(__file__).parent
json_file = base_dir / "source" / "IEEE_deep_learning_1825.json"
db_file = base_dir / "bd" / "ieee_deep_learning.db"

# Charger le JSON
with open(json_file, "r", encoding="utf-8") as f:
    data = json.load(f)

# Connexion SQLite
conn = sqlite3.connect(db_file)
cur = conn.cursor()

# Créer les tables principales
cur.executescript("""
CREATE TABLE IF NOT EXISTS articles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT,
    link TEXT,
    abstract TEXT,
    doi TEXT,
    date_publication TEXT,
    publisher TEXT,
    published_in TEXT,
    electronic_issn TEXT,
    print_issn TEXT
);

CREATE TABLE IF NOT EXISTS authors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS labs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lab_name TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS author_labs (
    author_id INTEGER,
    lab_id INTEGER,
    FOREIGN KEY(author_id) REFERENCES authors(id),
    FOREIGN KEY(lab_id) REFERENCES labs(id)
);

CREATE TABLE IF NOT EXISTS article_authors (
    article_id INTEGER,
    author_id INTEGER,
    FOREIGN KEY(article_id) REFERENCES articles(id),
    FOREIGN KEY(author_id) REFERENCES authors(id)
);

CREATE TABLE IF NOT EXISTS keywords (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    article_id INTEGER,
    type TEXT,
    keyword TEXT,
    FOREIGN KEY(article_id) REFERENCES articles(id)
);
""")

# Insérer les données
for article in data:
    details = article.get("Details", {})
    issn = article.get("issn_info", {})
    cur.execute("""
        INSERT INTO articles (title, link, abstract, doi, date_publication, publisher, published_in, electronic_issn, print_issn)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        article.get("Title"),
        article.get("Link"),
        article.get("Abstract"),
        details.get("DOI"),
        details.get("Date of Publication"),
        details.get("Publisher"),
        details.get("Published In"),
        issn.get("Electronic ISSN"),
        issn.get("Print ISSN")
    ))
    article_id = cur.lastrowid

    # Auteurs et laboratoires
    for author in article.get("authors_data", []):
        name = author.get("name")
        if not name:
            continue
        cur.execute("INSERT OR IGNORE INTO authors (name) VALUES (?)", (name,))
        cur.execute("SELECT id FROM authors WHERE name=?", (name,))
        author_id = cur.fetchone()[0]

        cur.execute("INSERT INTO article_authors (article_id, author_id) VALUES (?, ?)", (article_id, author_id))

        for lab in author.get("labs", []):
            cur.execute("INSERT OR IGNORE INTO labs (lab_name) VALUES (?)", (lab,))
            cur.execute("SELECT id FROM labs WHERE lab_name=?", (lab,))
            lab_id = cur.fetchone()[0]
            cur.execute("INSERT INTO author_labs (author_id, lab_id) VALUES (?, ?)", (author_id, lab_id))

    # Mots-clés
    keywords = article.get("keywords", {})
    for key_type, key_list in keywords.items():
        for kw in key_list:
            cur.execute("INSERT INTO keywords (article_id, type, keyword) VALUES (?, ?, ?)", (article_id, key_type, kw))

# Sauvegarder et fermer
conn.commit()
conn.close()

print(f"✅ Base SQLite créée avec succès : {db_file}")