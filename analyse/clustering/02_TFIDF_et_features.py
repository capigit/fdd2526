import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from scipy.sparse import hstack
import sqlite3

# Charger les mots-clés groupés
df_keywords_grouped = pd.read_csv('G:/Mon Drive/Projets/FDD/analyse/clustering/df_keywords_grouped.csv')

# Extraire nombre d'auteurs et pays du labo principal
conn = sqlite3.connect('bd/fusion_ieee.db')

df_authors = pd.read_sql("""
SELECT article_id, COUNT(author_id) AS nb_auteurs
FROM article_authors
GROUP BY article_id
""", conn)

df_country = pd.read_sql("""
SELECT aa.article_id, l.country
FROM article_authors aa
LEFT JOIN author_labs al ON aa.author_id = al.author_id
LEFT JOIN labs l ON al.lab_id = l.id
GROUP BY aa.article_id
""", conn)
conn.close()

# Fusion
df_features = df_keywords_grouped.merge(df_authors, on='article_id', how='left') \
                                 .merge(df_country, on='article_id', how='left')

# TF-IDF
tfidf = TfidfVectorizer(max_features=1000, stop_words='english')
X_keywords = tfidf.fit_transform(df_features['keyword'].fillna(''))

# Standardisation du nombre d'auteurs
df_features['nb_auteurs'] = df_features['nb_auteurs'].fillna(0)
scaler = StandardScaler()
X_num = scaler.fit_transform(df_features[['nb_auteurs']])

# One-Hot encoding des pays
encoder = OneHotEncoder(sparse_output=False)
X_country = encoder.fit_transform(df_features[['country']].fillna('Unknown'))

# Combiner toutes les features
X_final = hstack([X_keywords, X_num, X_country])
print(f"Shape finale des données pour clustering : {X_final.shape}")

# Sauvegarde pour les scripts suivants
import joblib
joblib.dump((df_features, X_final, tfidf, encoder), 'G:/Mon Drive/Projets/FDD/analyse/clustering/features.pkl')