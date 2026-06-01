from pathlib import Path
import pickle

import matplotlib.pyplot as plt
import pandas as pd
from scipy.sparse import issparse
from sklearn.decomposition import PCA
from sklearn.decomposition import TruncatedSVD
from sklearn.manifold import TSNE


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CLUSTERING_DIR = PROJECT_ROOT / "analyse" / "clustering"


def visualize_clusters(filename_features, filename_clusters, title="Clusters", perplexity=30):
    with open(filename_features, "rb") as f:
        data = pickle.load(f)
    X_tfidf = data["tfidf"]

    df_clusters = pd.read_csv(filename_clusters)
    labels = df_clusters["cluster"].to_numpy()

    if len(labels) != X_tfidf.shape[0]:
        raise ValueError(
            f"Incoherence entre features ({X_tfidf.shape[0]}) et labels ({len(labels)})"
        )

    if X_tfidf.shape[0] < 3:
        print(f"Pas assez d'observations pour visualiser : {title}")
        return

    if issparse(X_tfidf):
        n_components = min(50, X_tfidf.shape[0] - 1, X_tfidf.shape[1] - 1)
        X_pca = TruncatedSVD(n_components=n_components, random_state=42).fit_transform(X_tfidf)
    else:
        n_components = min(50, X_tfidf.shape[0] - 1, X_tfidf.shape[1])
        X_pca = PCA(n_components=n_components).fit_transform(X_tfidf)

    perplexity = min(perplexity, X_pca.shape[0] - 1)
    X_embedded = TSNE(
        n_components=2,
        random_state=42,
        perplexity=perplexity,
    ).fit_transform(X_pca)

    plt.figure(figsize=(12, 8))
    scatter = plt.scatter(X_embedded[:, 0], X_embedded[:, 1], c=labels, cmap="tab20", s=10)
    plt.legend(*scatter.legend_elements(), title="Clusters", bbox_to_anchor=(1.05, 1))
    plt.title(title)
    plt.xlabel("t-SNE 1")
    plt.ylabel("t-SNE 2")
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    visualize_clusters(
        CLUSTERING_DIR / "features.pkl",
        CLUSTERING_DIR / "clusters_articles.csv",
        title="Visualisation des clusters Articles",
    )

    visualize_clusters(
        CLUSTERING_DIR / "features_auteurs.pkl",
        CLUSTERING_DIR / "clusters_auteurs.csv",
        title="Visualisation des clusters Auteurs",
    )
