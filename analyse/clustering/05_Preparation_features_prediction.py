from pathlib import Path
import pickle

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CLUSTERING_DIR = PROJECT_ROOT / "analyse" / "clustering"
PREDICTION_DIR = PROJECT_ROOT / "analyse" / "prediction"


def main():
    df_article_clusters = pd.read_csv(CLUSTERING_DIR / "clusters_articles.csv")

    with open(CLUSTERING_DIR / "features.pkl", "rb") as f:
        data = pickle.load(f)
    df_features = data["features"]

    df_pred = df_features.merge(df_article_clusters, on="article_id", how="left")

    PREDICTION_DIR.mkdir(parents=True, exist_ok=True)
    df_pred.to_csv(PREDICTION_DIR / "features_prediction.csv", index=False)
    print(f"Export pret pour prediction : {df_pred.shape}")


if __name__ == "__main__":
    main()
