import pandas as pd
import pickle
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA
from sklearn.manifold import TSNE
import numpy as np

def visualize_clusters(filename_features, filename_clusters, title="Clusters", perplexity=30):
    # Chargement features TF-IDF
    data = pickle.load(open(filename_features, "rb"))
    X_tfidf = data["tfidf"]

    # Chargement clusters
    df_clusters = pd.read_csv(filename_clusters)
    labels = df_clusters['cluster'].to_numpy()

    # PCA pour réduire la dimension
    pca = PCA(n_components=50)
    X_pca = pca.fit_transform(X_tfidf)

    # t-SNE pour visualisation
    tsne = TSNE(n_components=2, random_state=42, perplexity=perplexity)
    X_embedded = tsne.fit_transform(X_pca)

    # Plot
    plt.figure(figsize=(12,8))
    scatter = plt.scatter(X_embedded[:,0], X_embedded[:,1], c=labels, cmap='tab20', s=10)
    plt.legend(*scatter.legend_elements(), title="Clusters", bbox_to_anchor=(1.05, 1))
    plt.title(title)
    plt.xlabel("t-SNE 1")
    plt.ylabel("t-SNE 2")
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    # Articles
    visualize_clusters(
        "analyse/clustering/features.pkl",
        "analyse/clustering/clusters_articles.csv",
        title="Visualisation des clusters Articles"
    )

    # Auteurs
    visualize_clusters(
        "analyse/clustering/features.pkl",
        "analyse/clustering/clusters_auteurs.csv",
        title="Visualisation des clusters Auteurs"
    )