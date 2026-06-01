import sqlite3
import pandas as pd

def main():
    db_path = "bd/fusion_ieee.db"
    conn = sqlite3.connect(db_path)

    # Lecture des tables
    tables = ['articles', 'authors', 'labs', 'keywords', 'article_authors', 'author_labs']
    for table in tables:
        df = pd.read_sql(f"SELECT * FROM {table}", conn)
        print(f"Table {table}: {df.shape[0]} lignes, {df.shape[1]} colonnes")
        print(df.head())
        print(df.dtypes)
        print(df.isna().sum())
        print('-'*50)
    
    conn.close()

if __name__ == "__main__":
    main()