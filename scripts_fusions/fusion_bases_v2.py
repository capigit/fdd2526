from pathlib import Path
import sqlite3
import unicodedata


PROJECT_ROOT = Path(__file__).resolve().parents[1]
BASE_DIR = PROJECT_ROOT / "bd"
FUSION_PATH = BASE_DIR / "fusion_ieee.db"

SOURCE_DBS = [
    "ieee_ai_articles.db",
    "ieee_llm_articles.db",
    "ieee_deep_learning.db",
    "ieee_nlp.db",
    "ieee_machine_learning.db",
]


def normalize_text(value):
    if value is None:
        return ""
    value = str(value).strip().lower()
    return unicodedata.normalize("NFD", value).encode("ascii", "ignore").decode("utf-8")


def clean_value(value):
    return None if value == "" else value


def article_dedupe_key(article):
    doi = normalize_text(article.get("doi"))
    if doi:
        return ("doi", doi)

    title = normalize_text(article.get("title"))
    if title:
        return ("title", title)

    return ("source_id", article.get("source"), article.get("source_article_id"))


def normalize_article_row(row):
    data = dict(row)
    source_article_id = data.pop("id", None)
    if "date_of_publication" in data:
        data["date_publication"] = data.pop("date_of_publication")

    return {
        "source_article_id": source_article_id,
        "title": data.get("title"),
        "link": data.get("link"),
        "abstract": data.get("abstract"),
        "doi": data.get("doi"),
        "date_publication": data.get("date_publication"),
        "publisher": data.get("publisher"),
        "published_in": data.get("published_in"),
        "print_issn": data.get("print_issn"),
        "electronic_issn": data.get("electronic_issn"),
    }


def extract_country(lab_name):
    if not lab_name or not isinstance(lab_name, str):
        return "Inconnu"

    country = lab_name.split(",")[-1].strip()
    return country if len(country) > 1 else "Inconnu"


def get_columns(cursor, table_name):
    cursor.execute(f"PRAGMA table_info({table_name})")
    return [column[1] for column in cursor.fetchall()]


def table_exists(cursor, table_name):
    cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    )
    return cursor.fetchone() is not None


def create_schema(cursor):
    cursor.executescript("""
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
        source TEXT,
        source_article_id INTEGER
    );

    CREATE TABLE authors (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        source TEXT,
        source_author_id INTEGER
    );

    CREATE TABLE labs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        lab_name TEXT,
        country TEXT,
        source TEXT,
        source_lab_id INTEGER
    );

    CREATE TABLE article_authors (
        article_id INTEGER,
        author_id INTEGER,
        source TEXT
    );

    CREATE TABLE author_labs (
        author_id INTEGER,
        lab_id INTEGER,
        source TEXT
    );

    CREATE TABLE keywords (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        article_id INTEGER,
        type TEXT,
        keyword TEXT,
        source TEXT,
        source_keyword_id INTEGER
    );

    CREATE INDEX idx_articles_source_id ON articles(source, source_article_id);
    CREATE INDEX idx_authors_source_id ON authors(source, source_author_id);
    CREATE INDEX idx_labs_source_id ON labs(source, source_lab_id);
    CREATE INDEX idx_keywords_article_id ON keywords(article_id);
    CREATE INDEX idx_article_authors_article ON article_authors(article_id);
    CREATE INDEX idx_article_authors_author ON article_authors(author_id);
    CREATE INDEX idx_author_labs_author ON author_labs(author_id);
    CREATE INDEX idx_author_labs_lab ON author_labs(lab_id);
    """)


def insert_article(cursor, article, source, dedupe_index):
    article = {**article, "source": source}
    key = article_dedupe_key(article)

    if key in dedupe_index:
        return dedupe_index[key], False

    cursor.execute(
        """
        INSERT INTO articles (
            title, link, abstract, doi, date_publication, publisher, published_in,
            print_issn, electronic_issn, source, source_article_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            article["title"],
            article["link"],
            article["abstract"],
            article["doi"],
            article["date_publication"],
            article["publisher"],
            article["published_in"],
            article["print_issn"],
            article["electronic_issn"],
            source,
            article["source_article_id"],
        ),
    )
    article_id = cursor.lastrowid
    dedupe_index[key] = article_id
    return article_id, True


def insert_author(cursor, source, source_author_id, name, author_map):
    key = (source, source_author_id)
    if source_author_id is not None and key in author_map:
        return author_map[key]

    cursor.execute(
        "INSERT INTO authors (name, source, source_author_id) VALUES (?, ?, ?)",
        (clean_value(name), source, source_author_id),
    )
    author_id = cursor.lastrowid
    if source_author_id is not None:
        author_map[key] = author_id
    return author_id


def insert_lab(cursor, source, source_lab_id, lab_name, lab_map):
    if not lab_name:
        return None

    key = (source, source_lab_id) if source_lab_id is not None else (source, normalize_text(lab_name))
    if key in lab_map:
        return lab_map[key]

    cursor.execute(
        "INSERT INTO labs (lab_name, country, source, source_lab_id) VALUES (?, ?, ?, ?)",
        (lab_name, extract_country(lab_name), source, source_lab_id),
    )
    lab_id = cursor.lastrowid
    lab_map[key] = lab_id
    return lab_id


def insert_relation(cursor, table, values, seen):
    if values in seen:
        return
    seen.add(values)

    if table == "article_authors":
        cursor.execute(
            "INSERT INTO article_authors (article_id, author_id, source) VALUES (?, ?, ?)",
            values,
        )
    elif table == "author_labs":
        cursor.execute(
            "INSERT INTO author_labs (author_id, lab_id, source) VALUES (?, ?, ?)",
            values,
        )


def import_articles(source_conn, fusion_cursor, source, article_map, article_dedupe_index):
    source_conn.row_factory = sqlite3.Row
    source_cur = source_conn.cursor()

    inserted = 0
    for row in source_cur.execute("SELECT * FROM articles"):
        article = normalize_article_row(row)
        source_article_id = article["source_article_id"]
        fusion_article_id, did_insert = insert_article(
            fusion_cursor,
            article,
            source,
            article_dedupe_index,
        )
        article_map[(source, source_article_id)] = fusion_article_id
        inserted += int(did_insert)

    return inserted


def import_authors_and_labs(source_conn, fusion_cursor, source, article_map, author_map, lab_map, seen_article_authors, seen_author_labs):
    source_conn.row_factory = sqlite3.Row
    source_cur = source_conn.cursor()
    author_columns = get_columns(source_cur, "authors")

    if "article_id" in author_columns and "lab" in author_columns:
        imported_authors = 0
        for row in source_cur.execute("SELECT * FROM authors"):
            source_author_id = row["id"]
            name = row["name"]
            if not name:
                continue

            author_id = insert_author(fusion_cursor, source, source_author_id, name, author_map)
            imported_authors += 1

            article_id = article_map.get((source, row["article_id"]))
            if article_id is not None:
                insert_relation(
                    fusion_cursor,
                    "article_authors",
                    (article_id, author_id, source),
                    seen_article_authors,
                )

            lab_id = insert_lab(fusion_cursor, source, None, row["lab"], lab_map)
            if lab_id is not None:
                insert_relation(
                    fusion_cursor,
                    "author_labs",
                    (author_id, lab_id, source),
                    seen_author_labs,
                )

        return imported_authors

    imported_authors = 0
    for row in source_cur.execute("SELECT * FROM authors"):
        source_author_id = row["id"]
        name = row["name"]
        if not name:
            continue
        insert_author(fusion_cursor, source, source_author_id, name, author_map)
        imported_authors += 1

    if table_exists(source_cur, "labs"):
        for row in source_cur.execute("SELECT * FROM labs"):
            insert_lab(fusion_cursor, source, row["id"], row["lab_name"], lab_map)

    if table_exists(source_cur, "article_authors"):
        for row in source_cur.execute("SELECT * FROM article_authors"):
            article_id = article_map.get((source, row["article_id"]))
            author_id = author_map.get((source, row["author_id"]))
            if article_id is not None and author_id is not None:
                insert_relation(
                    fusion_cursor,
                    "article_authors",
                    (article_id, author_id, source),
                    seen_article_authors,
                )

    if table_exists(source_cur, "author_labs"):
        for row in source_cur.execute("SELECT * FROM author_labs"):
            author_id = author_map.get((source, row["author_id"]))
            lab_id = lab_map.get((source, row["lab_id"]))
            if author_id is not None and lab_id is not None:
                insert_relation(
                    fusion_cursor,
                    "author_labs",
                    (author_id, lab_id, source),
                    seen_author_labs,
                )

    return imported_authors


def import_keywords(source_conn, fusion_cursor, source, article_map):
    source_conn.row_factory = sqlite3.Row
    source_cur = source_conn.cursor()

    inserted = 0
    for row in source_cur.execute("SELECT * FROM keywords"):
        article_id = article_map.get((source, row["article_id"]))
        if article_id is None:
            continue

        fusion_cursor.execute(
            """
            INSERT INTO keywords (article_id, type, keyword, source, source_keyword_id)
            VALUES (?, ?, ?, ?, ?)
            """,
            (article_id, row["type"], row["keyword"], source, row["id"]),
        )
        inserted += 1

    return inserted


def print_counts(cursor):
    print("\nTables fusionnees :")
    for table in ["articles", "authors", "labs", "article_authors", "author_labs", "keywords"]:
        count = cursor.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"- {table}: {count}")


def main():
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    if FUSION_PATH.exists():
        FUSION_PATH.unlink()

    fusion_conn = sqlite3.connect(FUSION_PATH)
    fusion_cur = fusion_conn.cursor()
    create_schema(fusion_cur)

    article_map = {}
    author_map = {}
    lab_map = {}
    article_dedupe_index = {}
    seen_article_authors = set()
    seen_author_labs = set()

    for source in SOURCE_DBS:
        db_path = BASE_DIR / source
        if not db_path.exists():
            print(f"Base manquante : {source}")
            continue

        print(f"\nFusion de {source}...")
        with sqlite3.connect(db_path) as source_conn:
            articles_inserted = import_articles(
                source_conn,
                fusion_cur,
                source,
                article_map,
                article_dedupe_index,
            )
            authors_inserted = import_authors_and_labs(
                source_conn,
                fusion_cur,
                source,
                article_map,
                author_map,
                lab_map,
                seen_article_authors,
                seen_author_labs,
            )
            keywords_inserted = import_keywords(source_conn, fusion_cur, source, article_map)

        fusion_conn.commit()
        print(f"  Articles nouveaux : {articles_inserted}")
        print(f"  Auteurs importes  : {authors_inserted}")
        print(f"  Keywords importes : {keywords_inserted}")

    print_counts(fusion_cur)
    fusion_conn.close()
    print(f"\nFusion terminee : {FUSION_PATH}")


if __name__ == "__main__":
    main()
