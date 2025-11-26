import pandas as pd
import pickle

def main():
    # Clusters Articles
    df_article_clusters = pd.read_csv("analyse/clustering/clusters_articles.csv")
    
    # Clusters Auteurs
    df_author_clusters = pd.read_csv("analyse/clustering/clusters_auteurs.csv")

    # Features TF-IDF + num. articles pour prédiction
    data = pickle.load(open("analyse/clustering/features.pkl", "rb"))
    df_features = data["features"]

    # Merge clusters avec features
    df_pred = df_features.merge(df_article_clusters, on="article_id", how="left")
    
    # Export CSV prêt pour modèle prédictif
    df_pred.to_csv("analyse/prediction/features_prediction.csv", index=False)
    print(f"Export prêt pour prédiction : {df_pred.shape}")

if __name__ == "__main__":
    main()