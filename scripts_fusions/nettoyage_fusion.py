from pathlib import Path
import sqlite3
import unicodedata


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "bd" / "fusion_ieee.db"


def normalize_text(value):
    if not value or not isinstance(value, str):
        return ""
    value = value.lower().strip()
    return unicodedata.normalize("NFD", value).encode("ascii", "ignore").decode("utf-8")


def extract_country(lab_name):
    if not lab_name or not isinstance(lab_name, str):
        return "Inconnu"
    country = lab_name.split(",")[-1].strip()
    return country if len(country) > 1 else "Inconnu"


def column_exists(cursor, table, column):
    cursor.execute(f"PRAGMA table_info({table})")
    return column in [row[1] for row in cursor.fetchall()]


def normalize_articles(cursor):
    print("Nettoyage des articles...")
    cursor.execute("SELECT id, title, publisher, published_in, doi FROM articles")
    for article_id, title, publisher, published_in, doi in cursor.fetchall():
        cursor.execute(
            """
            UPDATE articles
            SET title = ?, publisher = ?, published_in = ?, doi = ?
            WHERE id = ?
            """,
            (
                normalize_text(title),
                normalize_text(publisher),
                normalize_text(published_in),
                normalize_text(doi),
                article_id,
            ),
        )


def normalize_keywords(cursor):
    print("Nettoyage des keywords...")
    cursor.execute("SELECT id, keyword FROM keywords")
    for keyword_id, keyword in cursor.fetchall():
        cursor.execute(
            "UPDATE keywords SET keyword = ? WHERE id = ?",
            (normalize_text(keyword), keyword_id),
        )


def normalize_authors(cursor):
    print("Nettoyage des auteurs...")
    cursor.execute("SELECT id, name FROM authors")
    empty_author_ids = []

    for author_id, name in cursor.fetchall():
        name_normalized = normalize_text(name)
        if not name_normalized:
            empty_author_ids.append(author_id)
            continue
        cursor.execute("UPDATE authors SET name = ? WHERE id = ?", (name_normalized, author_id))

    if empty_author_ids:
        placeholders = ",".join(["?"] * len(empty_author_ids))
        cursor.execute(f"DELETE FROM article_authors WHERE author_id IN ({placeholders})", empty_author_ids)
        cursor.execute(f"DELETE FROM author_labs WHERE author_id IN ({placeholders})", empty_author_ids)
        cursor.execute(f"DELETE FROM authors WHERE id IN ({placeholders})", empty_author_ids)
        print(f"Auteurs vides supprimes : {len(empty_author_ids)}")


def normalize_labs(cursor):
    print("Nettoyage des laboratoires...")
    if not column_exists(cursor, "labs", "country"):
        cursor.execute("ALTER TABLE labs ADD COLUMN country TEXT")

    cursor.execute("SELECT id, lab_name FROM labs")
    for lab_id, lab_name in cursor.fetchall():
        cursor.execute(
            "UPDATE labs SET lab_name = ?, country = ? WHERE id = ?",
            (lab_name.strip() if isinstance(lab_name, str) else lab_name, extract_country(lab_name), lab_id),
        )


def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    normalize_articles(cursor)
    normalize_keywords(cursor)
    normalize_authors(cursor)
    normalize_labs(cursor)

    conn.commit()
    conn.close()
    print("\nNettoyage termine sans suppression d'articles.")


if __name__ == "__main__":
    main()
