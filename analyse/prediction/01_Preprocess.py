from pathlib import Path

import joblib
import pandas as pd
from scipy.sparse import csr_matrix, hstack, save_npz
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import OneHotEncoder


MIN_CLUSTER_SIZE = 2
FEATURES_TO_USE = ["keyword", "nb_auteurs", "country"]

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PREDICTION_DIR = PROJECT_ROOT / "analyse" / "prediction"
PREDICTION_DIR.mkdir(parents=True, exist_ok=True)


def make_one_hot_encoder():
    try:
        return OneHotEncoder(sparse_output=True, handle_unknown="ignore")
    except TypeError:
        return OneHotEncoder(sparse=True, handle_unknown="ignore")


def main():
    df = pd.read_csv(PREDICTION_DIR / "features_prediction.csv")
    print("Shape initiale :", df.shape)

    cluster_counts = df["cluster"].value_counts()
    small_clusters = cluster_counts[cluster_counts < MIN_CLUSTER_SIZE].index.tolist()
    if small_clusters:
        print("Clusters trop petits supprimes:", small_clusters)
        df = df[~df["cluster"].isin(small_clusters)]

    y = df["cluster"]
    X = df[FEATURES_TO_USE].copy()
    print("Features utilisees :", FEATURES_TO_USE)

    feature_matrices = []

    tfidf = TfidfVectorizer(max_features=1000)
    X_tfidf = tfidf.fit_transform(X["keyword"].fillna("").astype(str))
    feature_matrices.append(X_tfidf)

    X_num = X[["nb_auteurs"]].fillna(0).values
    feature_matrices.append(csr_matrix(X_num))

    ohe = make_one_hot_encoder()
    X_country = ohe.fit_transform(X[["country"]].fillna("Unknown").astype(str))
    feature_matrices.append(X_country)

    X_final = hstack(feature_matrices).tocsr()
    print("Shape final :", X_final.shape)

    save_npz(PREDICTION_DIR / "X_prediction.npz", X_final)
    y.to_csv(PREDICTION_DIR / "y_prediction.csv", index=False)
    joblib.dump(tfidf, PREDICTION_DIR / "tfidf_transformer.pkl")
    joblib.dump(ohe, PREDICTION_DIR / "ohe_country.pkl")
    joblib.dump({"features_to_use": FEATURES_TO_USE}, PREDICTION_DIR / "feature_config.pkl")

    print("Pretraitement termine.")


if __name__ == "__main__":
    main()
