import sqlite3
import pandas as pd

def main():
    db_path = "bd/fusion_ieee.db"
    conn = sqlite3.connect(db_path)

    # Vérifier doublons et relations
    df_article_authors = pd.read_sql("SELECT * FROM article_authors", conn)
    df_author_labs = pd.read_sql("SELECT * FROM author_labs", conn)
    df_authors = pd.read_sql("SELECT * FROM authors", conn)
    df_labs = pd.read_sql("SELECT * FROM labs", conn)
    df_articles = pd.read_sql("SELECT * FROM articles", conn)

    print("Doublons:")
    for df, name in zip([df_articles, df_authors, df_labs, df_article_authors, df_author_labs],
                        ['articles', 'authors', 'labs', 'article_authors', 'author_labs']):
        print(f"{name}: {df.duplicated().sum()} doublons")

    # Relations invalides
    missing_authors = set(df_article_authors['author_id']) - set(df_authors['id'])
    missing_articles = set(df_article_authors['article_id']) - set(df_articles['id'])
    missing_labs = set(df_author_labs['lab_id']) - set(df_labs['id'])
    missing_author_labs = set(df_author_labs['author_id']) - set(df_authors['id'])

    print(f"Auteurs manquants dans article_authors: {len(missing_authors)}")
    print(f"Articles manquants dans article_authors: {len(missing_articles)}")
    print(f"Labos manquants dans author_labs: {len(missing_labs)}")
    print(f"Auteurs manquants dans author_labs: {len(missing_author_labs)}")

    conn.close()

if __name__ == "__main__":
    main()