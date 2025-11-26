import sqlite3
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
import pickle
import numpy as np

def main():
    conn = sqlite3.connect("bd/fusion_ieee.db")
    
    # On récupère les articles par auteur et les keywords
    df_article_authors = pd.read_sql("SELECT * FROM article_authors", conn)
    df_keywords = pd.read_sql("SELECT article_id, keyword FROM keywords", conn)
    
    # Merge pour avoir keywords par auteur
    df_author_keywords = df_article_authors.merge(df_keywords, on="article_id", how="left")
    df_author_keywords_grouped = df_author_keywords.groupby("author_id")["keyword"].apply(lambda x: " ".join(x)).reset_index()
    
    # TF-IDF
    tfidf = TfidfVectorizer(max_features=1000)
    X_tfidf = tfidf.fit_transform(df_author_keywords_grouped['keyword']).toarray()
    
    # KMeans
    kmeans = KMeans(n_clusters=50, random_state=42)
    labels = kmeans.fit_predict(X_tfidf)
    df_author_keywords_grouped['cluster'] = labels
    
    # Export CSV
    df_author_keywords_grouped[['author_id', 'cluster']].to_csv("analyse/clustering/clusters_auteurs.csv", index=False)
    print("Clustering auteurs terminé et exporté.")
    
    conn.close()

if __name__ == "__main__":
    main()