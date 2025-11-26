import pickle
from sklearn.cluster import KMeans
import pandas as pd

def main():
    # Chargement features
    data = pickle.load(open("analyse/clustering/features.pkl", "rb"))
    X_tfidf = data["tfidf"]
    df_features = data["features"]

    # On peut combiner TF-IDF et features numériques si voulu
    import numpy as np
    X_numeric = df_features[['nb_auteurs']].to_numpy()
    X = np.hstack([X_tfidf, X_numeric])

    # KMeans
    kmeans = KMeans(n_clusters=50, random_state=42)
    labels = kmeans.fit_predict(X)
    df_features['cluster'] = labels

    # Export CSV
    df_features[['article_id', 'cluster']].to_csv("analyse/clustering/clusters_articles.csv", index=False)
    print("Clustering articles terminé et exporté.")

if __name__ == "__main__":
    main()