import sqlite3
import pandas as pd

def main():
    db_path = "bd/fusion_ieee.db"
    conn = sqlite3.connect(db_path)

    df_articles = pd.read_sql("SELECT * FROM articles", conn)
    df_authors = pd.read_sql("SELECT * FROM authors", conn)
    df_labs = pd.read_sql("SELECT * FROM labs", conn)
    df_keywords = pd.read_sql("SELECT * FROM keywords", conn)

    print("Nombre total:")
    print(f"Articles: {len(df_articles)}")
    print(f"Auteurs: {len(df_authors)}")
    print(f"Labs: {len(df_labs)}")
    print(f"Keywords: {len(df_keywords)}")

    # Articles par année
    if 'year' in df_articles.columns:
        print("\nArticles par année:")
        print(df_articles.groupby('year').size())

    # Auteurs par pays
    df_author_labs = pd.read_sql("SELECT * FROM author_labs", conn)
    df_country = pd.read_sql("SELECT id, country FROM labs", conn)
    df_authors_country = pd.merge(df_author_labs, df_country, left_on='lab_id', right_on='id', how='left')
    print("\nNombre d'auteurs par pays:")
    print(df_authors_country.groupby('country')['author_id'].nunique())

    # Répartition labs par pays
    print("\nNombre de labs par pays:")
    print(df_country['country'].value_counts())

    conn.close()

if __name__ == "__main__":
    main()