from pathlib import Path
import pickle

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CLUSTERING_DIR = PROJECT_ROOT / "analyse" / "clustering"
EDA_DIR = PROJECT_ROOT / "analyse" / "EDA"


def main():
    df_keywords = pd.read_csv(CLUSTERING_DIR / "df_keywords_grouped.csv")

    tfidf = TfidfVectorizer(max_features=1000)
    X_tfidf = tfidf.fit_transform(df_keywords["keyword"].fillna("").astype(str)).toarray()
    print(f"TF-IDF matrix shape: {X_tfidf.shape}")

    df_stats = pd.read_csv(EDA_DIR / "stats_descriptives.csv")
    df_features = pd.merge(df_keywords, df_stats, on="article_id", how="left")
    df_features["nb_auteurs"] = df_features["nb_auteurs"].fillna(0)
    df_features["country"] = df_features["country"].fillna("Unknown")

    with open(CLUSTERING_DIR / "features.pkl", "wb") as f:
        pickle.dump({"tfidf": X_tfidf, "features": df_features}, f)
    print(f"Features pretes : {df_features.shape}")


if __name__ == "__main__":
    main()
