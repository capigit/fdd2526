from pathlib import Path
import pickle
import sqlite3

import pandas as pd
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "bd" / "fusion_ieee.db"
CLUSTERING_DIR = PROJECT_ROOT / "analyse" / "clustering"
N_CLUSTERS = 50


def main():
    conn = sqlite3.connect(DB_PATH)
    df_article_authors = pd.read_sql("SELECT * FROM article_authors", conn)
    df_keywords = pd.read_sql("SELECT article_id, keyword FROM keywords", conn)
    conn.close()

    df_author_keywords = df_article_authors.merge(df_keywords, on="article_id", how="left")
    df_author_keywords["keyword"] = df_author_keywords["keyword"].fillna("").astype(str)
    df_author_keywords_grouped = (
        df_author_keywords
        .groupby("author_id")["keyword"]
        .apply(lambda x: " ".join(x))
        .reset_index()
    )

    tfidf = TfidfVectorizer(max_features=1000)
    X_tfidf = tfidf.fit_transform(df_author_keywords_grouped["keyword"])

    n_clusters = min(N_CLUSTERS, len(df_author_keywords_grouped))
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    labels = kmeans.fit_predict(X_tfidf)
    df_author_keywords_grouped["cluster"] = labels

    df_author_keywords_grouped[["author_id", "cluster"]].to_csv(
        CLUSTERING_DIR / "clusters_auteurs.csv",
        index=False,
    )
    with open(CLUSTERING_DIR / "features_auteurs.pkl", "wb") as f:
        pickle.dump({"tfidf": X_tfidf, "features": df_author_keywords_grouped}, f)

    print("Clustering auteurs termine et exporte.")


if __name__ == "__main__":
    main()
