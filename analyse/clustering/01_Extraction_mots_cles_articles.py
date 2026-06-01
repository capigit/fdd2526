from pathlib import Path
import sqlite3

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "bd" / "fusion_ieee.db"
CLUSTERING_DIR = PROJECT_ROOT / "analyse" / "clustering"


def main():
    conn = sqlite3.connect(DB_PATH)
    df_keywords = pd.read_sql("SELECT article_id, keyword FROM keywords", conn)
    conn.close()

    df_keywords_grouped = (
        df_keywords
        .assign(keyword=df_keywords["keyword"].fillna("").astype(str))
        .groupby("article_id")["keyword"]
        .apply(lambda x: " ".join(x))
        .reset_index()
    )

    CLUSTERING_DIR.mkdir(parents=True, exist_ok=True)
    df_keywords_grouped.to_csv(CLUSTERING_DIR / "df_keywords_grouped.csv", index=False)
    print(f"Extraction terminee : {len(df_keywords_grouped)} articles avec mots-cles")


if __name__ == "__main__":
    main()
