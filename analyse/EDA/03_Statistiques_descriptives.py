import sqlite3
import pandas as pd

def main():
    db_path = "bd/fusion_ieee.db"
    conn = sqlite3.connect(db_path)

    # Lecture des tables
    df_articles = pd.read_sql("SELECT id, title, date_publication FROM articles", conn)
    df_authors = pd.read_sql("SELECT id FROM authors", conn)
    df_labs = pd.read_sql("SELECT id, country FROM labs", conn)
    df_keywords = pd.read_sql("SELECT * FROM keywords", conn)
    df_article_authors = pd.read_sql("SELECT * FROM article_authors", conn)
    df_author_labs = pd.read_sql("SELECT * FROM author_labs", conn)

    print("Nombre total:")
    print(f"Articles: {len(df_articles)}")
    print(f"Auteurs: {len(df_authors)}")
    print(f"Labs: {len(df_labs)}")
    print(f"Keywords: {len(df_keywords)}")

    # Articles par année
    df_articles['year'] = pd.to_datetime(df_articles['date_publication'], errors='coerce').dt.year
    print("\nArticles par année:")
    print(df_articles.groupby('year').size())

    # Auteurs par pays
    df_authors_country = pd.merge(df_author_labs, df_labs, left_on='lab_id', right_on='id', how='left')
    print("\nNombre d'auteurs par pays:")
    print(df_authors_country.groupby('country')['author_id'].nunique())

    # Répartition labs par pays
    print("\nNombre de labs par pays:")
    print(df_labs['country'].value_counts())

    # === Construction du CSV pour clustering ===
    # Nombre d'auteurs par article
    df_nb_auteurs = df_article_authors.groupby('article_id')['author_id'].count().reset_index()
    df_nb_auteurs.rename(columns={'author_id': 'nb_auteurs'}, inplace=True)

    # Pays majoritaire des auteurs par article
    df_article_country = df_article_authors.merge(df_authors_country[['author_id', 'country']], on='author_id', how='left')
    df_article_country = df_article_country.groupby('article_id')['country'].agg(
        lambda x: x.mode()[0] if len(x) > 0 and len(x.mode()) > 0 else "Unknown"
    ).reset_index()

    # Merge pour obtenir stats descriptives par article
    df_stats = df_articles[['id']].merge(df_nb_auteurs, left_on='id', right_on='article_id', how='left')
    df_stats = df_stats.merge(df_article_country, left_on='id', right_on='article_id', how='left')

    # Nettoyage et renommage colonnes
    df_stats = df_stats.rename(columns={'id': 'article_id'})
    df_stats = df_stats[['article_id', 'nb_auteurs', 'country']]
    df_stats['nb_auteurs'] = df_stats['nb_auteurs'].fillna(0)
    df_stats['country'] = df_stats['country'].fillna("Unknown")

    # Export CSV
    df_stats.to_csv("analyse/EDA/stats_descriptives.csv", index=False)
    print("\nCSV 'stats_descriptives.csv' généré avec succès !")

    conn.close()

if __name__ == "__main__":
    main()