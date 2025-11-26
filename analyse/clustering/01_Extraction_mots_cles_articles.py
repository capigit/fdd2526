import sqlite3
import pandas as pd

def main():
    conn = sqlite3.connect("bd/fusion_ieee.db")

    # On récupère keywords par article
    df_keywords = pd.read_sql("SELECT article_id, keyword FROM keywords", conn)
    
    # On regroupe tous les keywords d'un article en une seule string
    df_keywords_grouped = df_keywords.groupby("article_id")["keyword"].apply(lambda x: " ".join(x)).reset_index()
    
    # Export CSV intermédiaire
    df_keywords_grouped.to_csv("analyse/clustering/df_keywords_grouped.csv", index=False)
    print(f"Extraction terminée : {len(df_keywords_grouped)} articles avec mots-clés")
    
    conn.close()

if __name__ == "__main__":
    main()