"""
01_Construction_features.py
- Construit X (features) et y (target) pour la prédiction du pays du labo principal d'un article.
- Features:
    * TF-IDF sur mots-clés groupés par article
    * TF-IDF optionnel sur abstract (désactivé par défaut)
    * nb_auteurs
    * longueur_abstract
- Stratégie cible: réduire le nombre de classes en gardant les pays fréquents (>= min_articles)
- Sauvegardes:
    - features.pkl -> (df_features, X_sparse, vectorizers_dict, label_encoder)
    - df_features.csv (pour inspection)
"""

import sqlite3
import pandas as pd
import numpy as np
import joblib
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import StandardScaler
from scipy.sparse import hstack, csr_matrix

DB_PATH = r"bd/fusion_ieee.db"
OUT_DIR = r"G:/Mon Drive/Projets/FDD/analyse/prediction"

# Paramètres
MAX_FEATURES_TFIDF = 2000     # mots-clés
MIN_COUNTRY_ARTICLES = 50     # seuil pour garder une classe; autres -> "Other"
USE_ABSTRACT_TFIDF = False    # si True, ajoute TF-IDF des abstracts

def main():
    conn = sqlite3.connect(DB_PATH)

    # 1) Récupérer mots-clés groupés par article
    df_kw = pd.read_sql("SELECT article_id, keyword FROM keywords", conn)
    df_kw_grouped = df_kw.groupby("article_id")["keyword"].apply(lambda x: " ".join(x)).reset_index()
    df_kw_grouped.rename(columns={"keyword": "keywords_text"}, inplace=True)

    # 2) Récupérer abstracts et articles
    df_articles = pd.read_sql("SELECT id as article_id, title, abstract FROM articles", conn)

    # 3) nombre d'auteurs par article
    df_na = pd.read_sql("""
        SELECT article_id, COUNT(author_id) AS nb_auteurs
        FROM article_authors
        GROUP BY article_id
    """, conn)

    # 4) country du labo principal (group by article_id)
    df_country = pd.read_sql("""
        SELECT aa.article_id, l.country
        FROM article_authors aa
        LEFT JOIN author_labs al ON aa.author_id = al.author_id
        LEFT JOIN labs l ON al.lab_id = l.id
        GROUP BY aa.article_id
    """, conn)

    conn.close()

    # 5) Fusionner
    df = df_articles.merge(df_kw_grouped, on="article_id", how="left") \
                    .merge(df_na, on="article_id", how="left") \
                    .merge(df_country, on="article_id", how="left")

    df['keywords_text'] = df['keywords_text'].fillna('')
    df['nb_auteurs'] = df['nb_auteurs'].fillna(0).astype(int)
    df['abstract'] = df['abstract'].fillna('')
    df['len_abstract'] = df['abstract'].apply(lambda x: len(x.split()))

    # 6) Filtrer articles ayant au moins 1 mot-clé (si tu veux inclure tous, commente la ligne suivante)
    df = df.reset_index(drop=True)

    # 7) Gérer les classes pays: garder les pays fréquents, autres -> Other
    country_counts = df['country'].value_counts(dropna=True)
    frequent_countries = set(country_counts[country_counts >= MIN_COUNTRY_ARTICLES].index.tolist())
    df['country_clean'] = df['country'].where(df['country'].isin(frequent_countries), other='Other')
    df['country_clean'] = df['country_clean'].fillna('Other')

    # 8) TF-IDF sur mots-clés
    tfidf_kw = TfidfVectorizer(max_features=MAX_FEATURES_TFIDF, stop_words='english')
    X_kw = tfidf_kw.fit_transform(df['keywords_text'])

    # 9) Optionnel: TF-IDF abstract
    if USE_ABSTRACT_TFIDF:
        tfidf_abs = TfidfVectorizer(max_features=2000, stop_words='english')
        X_abs = tfidf_abs.fit_transform(df['abstract'])
    else:
        tfidf_abs = None
        X_abs = None

    # 10) Features numériques (nb_auteurs, len_abstract) -> standardiser
    scaler = StandardScaler()
    X_num = scaler.fit_transform(df[['nb_auteurs', 'len_abstract']].fillna(0))

    # 11) Combiner en matrice sparse
    if X_abs is not None:
        X_sparse = hstack([X_kw, X_abs, csr_matrix(X_num)])
    else:
        X_sparse = hstack([X_kw, csr_matrix(X_num)])

    # 12) Label encoding
    from sklearn.preprocessing import LabelEncoder
    le = LabelEncoder()
    y = le.fit_transform(df['country_clean'])

    # 13) Sauvegardes
    joblib.dump({
        "df_features": df,
        "X": X_sparse,
        "tfidf_kw": tfidf_kw,
        "tfidf_abs": tfidf_abs,
        "scaler_num": scaler,
        "label_encoder": le
    }, f"{OUT_DIR}/features.pkl")

    df.to_csv(f"{OUT_DIR}/df_features.csv", index=False)

    print("Features construites et sauvegardées :")
    print(f" - articles total : {df.shape[0]}")
    print(f" - classes retenues : {sorted(df['country_clean'].value_counts().index.tolist())}")
    print(f" - shape X : {X_sparse.shape}")

if __name__ == "__main__":
    main()