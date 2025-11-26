import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer

def main():
    # Chargement des keywords regroupés
    df_keywords = pd.read_csv("analyse/clustering/df_keywords_grouped.csv")
    
    # TF-IDF
    tfidf = TfidfVectorizer(max_features=1000)
    X_tfidf = tfidf.fit_transform(df_keywords['keyword']).toarray()
    print(f"TF-IDF matrix shape: {X_tfidf.shape}")

    # Features numériques simples (nb auteurs, pays)
    df_stats = pd.read_csv("analyse/EDA/stats_descriptives.csv")
    df_features = pd.merge(df_keywords, df_stats, left_on="article_id", right_on="article_id", how="left")

    # Normalisation ou traitement NaN
    df_features['nb_auteurs'] = df_features['nb_auteurs'].fillna(0)

    # Export features
    import pickle
    pickle.dump({"tfidf": X_tfidf, "features": df_features}, open("analyse/clustering/features.pkl", "wb"))
    print(f"Features prêtes : {df_features.shape}")

if __name__ == "__main__":
    main()