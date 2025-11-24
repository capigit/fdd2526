import sqlite3
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from scipy.sparse import hstack
from sklearn.cluster import KMeans
import joblib

# Exemple simple : auteurs représentés par TF-IDF de leurs mots-clés
conn = sqlite3.connect('bd/fusion_ieee.db')
df_author_keywords = pd.read_sql("""
SELECT aa.author_id, k.keyword
FROM article_authors aa
LEFT JOIN keywords k ON aa.article_id = k.article_id
""", conn)
conn.close()

# Grouper par auteur
df_author_keywords_grouped = df_author_keywords.groupby('author_id')['keyword'].apply(lambda x: ' '.join(x)).reset_index()

# TF-IDF
tfidf = TfidfVectorizer(max_features=1000, stop_words='english')
X_keywords = tfidf.fit_transform(df_author_keywords_grouped['keyword'].fillna(''))

# K-means auteurs
K = 5
kmeans = KMeans(n_clusters=K, random_state=42, n_init=10)
df_author_keywords_grouped['cluster'] = kmeans.fit_predict(X_keywords)

# Sauvegarde
joblib.dump((df_author_keywords_grouped, kmeans, tfidf), 'G:/Mon Drive/Projets/FDD/analyse/clustering/kmeans_auteurs.pkl')
print(df_author_keywords_grouped.groupby('cluster').size())