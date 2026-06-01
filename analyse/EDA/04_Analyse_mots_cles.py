"""
Analyse exploratoire des mots-clés
- Top 20 mots-clés
- Wordcloud
- Analyse simple des termes dominants
"""

import sqlite3
import pandas as pd
from collections import Counter
from wordcloud import WordCloud
import matplotlib.pyplot as plt

# Connexion à la base
db_path = "bd/fusion_ieee.db"
conn = sqlite3.connect(db_path)

# Extraire les mots-clés
query = "SELECT article_id, keyword FROM keywords"
df_keywords = pd.read_sql(query, conn)
conn.close()

# Grouper les mots-clés par article
df_grouped = df_keywords.groupby('article_id')['keyword'].apply(lambda x: ' '.join(x)).reset_index()

# Comptage global des mots-clés
all_keywords = ' '.join(df_grouped['keyword']).split()
meaningful_words = [k.lower() for k in all_keywords if len(k) > 2]
counter = Counter(meaningful_words)

print("Top 20 mots-clés :")
for word, freq in counter.most_common(20):
    print(f"{word}: {freq}")

# Wordcloud
wordcloud = WordCloud(width=800, height=400, background_color='white').generate(' '.join(meaningful_words))
plt.figure(figsize=(15,6))
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis('off')
plt.title("Wordcloud des mots-clés")
plt.show()