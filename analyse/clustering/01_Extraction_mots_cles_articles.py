import sqlite3
import pandas as pd

# Connexion à la base
conn = sqlite3.connect('bd/fusion_ieee.db')

# Extraire les mots-clés associés aux articles
query = "SELECT article_id, keyword FROM keywords"
df_keywords = pd.read_sql(query, conn)
conn.close()

# Grouper les mots-clés par article
df_keywords_grouped = df_keywords.groupby('article_id')['keyword'].apply(lambda x: ' '.join(x)).reset_index()
df_keywords_grouped.to_csv('G:/Mon Drive/Projets/FDD/analyse/clustering/df_keywords_grouped.csv', index=False)
print(f"Extraction terminée : {df_keywords_grouped.shape[0]} articles avec mots-clés")