from pathlib import Path
import pickle

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CLUSTERING_DIR = PROJECT_ROOT / "analyse" / "clustering"
N_CLUSTERS = 50


def main():
    with open(CLUSTERING_DIR / "features.pkl", "rb") as f:
        data = pickle.load(f)

    X_tfidf = data["tfidf"]
    df_features = data["features"]

    X_numeric = df_features[["nb_auteurs"]].fillna(0).to_numpy()
    X = np.hstack([X_tfidf, X_numeric])

    n_clusters = min(N_CLUSTERS, len(df_features))
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    labels = kmeans.fit_predict(X)
    df_features["cluster"] = labels

    df_features[["article_id", "cluster"]].to_csv(
        CLUSTERING_DIR / "clusters_articles.csv",
        index=False,
    )
    print("Clustering articles termine et exporte.")


if __name__ == "__main__":
    main()
