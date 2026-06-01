from pathlib import Path

import joblib
import pandas as pd
from scipy.sparse import csr_matrix, hstack


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PREDICTION_DIR = PROJECT_ROOT / "analyse" / "prediction"


def build_features(new_data, tfidf, ohe):
    X_tfidf = tfidf.transform(new_data["keyword"].fillna("").astype(str))
    X_num = csr_matrix(new_data[["nb_auteurs"]].fillna(0).values)
    X_country = ohe.transform(new_data[["country"]].fillna("Unknown").astype(str))
    return hstack([X_tfidf, X_num, X_country])


def main():
    model = joblib.load(PREDICTION_DIR / "rf_model_prediction.pkl")
    tfidf = joblib.load(PREDICTION_DIR / "tfidf_transformer.pkl")
    ohe = joblib.load(PREDICTION_DIR / "ohe_country.pkl")
    print("Modele et transformateurs charges.")

    new_data = pd.DataFrame([{
        "article_id": 3881,
        "keyword": "artificial intelligence medical healthcare deep learning",
        "nb_auteurs": 3,
        "country": "Italy",
    }])

    X_features = build_features(new_data, tfidf, ohe)
    pred_cluster = model.predict(X_features)[0]
    print(f"Cluster predit pour l'article {new_data['article_id'].iloc[0]} :", pred_cluster)


if __name__ == "__main__":
    main()
