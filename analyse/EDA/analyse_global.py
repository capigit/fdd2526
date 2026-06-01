import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import networkx as nx
from pyvis.network import Network

sns.set(style="whitegrid")

# ------------------------------
# 1️⃣ Connexion à la base SQLite
# ------------------------------
db_path = r"bd/fusion_ieee.db"  # <- remplacer par le chemin réel
conn = sqlite3.connect(db_path)

# ------------------------------
# 2️⃣ Chargement des tables
# ------------------------------
articles = pd.read_sql("SELECT * FROM articles", conn)
authors = pd.read_sql("SELECT * FROM authors", conn)
article_authors = pd.read_sql("SELECT * FROM article_authors", conn)
keywords = pd.read_sql("SELECT * FROM keywords", conn)
author_labs = pd.read_sql("SELECT * FROM author_labs", conn)
labs = pd.read_sql("SELECT * FROM labs", conn)

# ------------------------------
# 3️⃣ Aperçu général
# ------------------------------
print("Nombre d'articles :", len(articles))
print("Nombre d'auteurs :", len(authors))
print("Nombre de laboratoires :", len(labs))
print("Nombre de mots-clés :", len(keywords))
print("\nValeurs manquantes par table :")
for df, name in zip([articles, authors, labs, keywords], ['articles', 'authors', 'labs', 'keywords']):
    print(f"{name}:\n", df.isnull().sum(), "\n")

# ------------------------------
# 4️⃣ Analyse temporelle
# ------------------------------
articles['year'] = pd.to_datetime(articles['date_publication'], errors='coerce').dt.year
year_counts = articles.groupby('year').size().reset_index(name='count')

plt.figure(figsize=(10,5))
sns.lineplot(data=year_counts, x='year', y='count', marker='o')
plt.title("Évolution des publications IA/ML par année")
plt.xlabel("Année")
plt.ylabel("Nombre d'articles")
plt.xticks(year_counts['year'].dropna().astype(int))
plt.show()

# ------------------------------
# 5️⃣ Analyse des mots-clés
# ------------------------------
top_keywords = keywords['keyword'].value_counts().head(20)
plt.figure(figsize=(10,6))
sns.barplot(x=top_keywords.values, y=top_keywords.index)
plt.title("Top 20 des mots-clés")
plt.xlabel("Nombre d'occurrences")
plt.ylabel("Mots-clés")
plt.show()

# Evolution des mots-clés émergents par année
keywords_articles = keywords.merge(articles[['id','year']], left_on='article_id', right_on='id')
keywords_trend = keywords_articles.groupby(['year','keyword']).size().reset_index(name='count')
top_keywords_list = top_keywords.index.tolist()
keywords_trend_top = keywords_trend[keywords_trend['keyword'].isin(top_keywords_list)]

plt.figure(figsize=(12,6))
sns.lineplot(data=keywords_trend_top, x='year', y='count', hue='keyword', marker='o')
plt.title("Évolution temporelle des mots-clés dominants")
plt.xlabel("Année")
plt.ylabel("Nombre d'occurrences")
plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
plt.show()

# ------------------------------
# 6️⃣ Analyse des auteurs et co-auteurs
# ------------------------------
# Top auteurs
top_authors = article_authors.merge(authors, left_on='author_id', right_on='id')
top_authors_count = top_authors['name'].value_counts().head(20)
plt.figure(figsize=(10,6))
sns.barplot(x=top_authors_count.values, y=top_authors_count.index)
plt.title("Top 20 des auteurs par nombre d'articles")
plt.xlabel("Nombre d'articles")
plt.ylabel("Auteur")
plt.show()

# Graphe co-auteurs
coauthor_edges = article_authors.merge(authors, left_on='author_id', right_on='id')[['article_id','name']]
edges = []
for aid in coauthor_edges['article_id'].unique():
    names = coauthor_edges[coauthor_edges['article_id']==aid]['name'].tolist()
    for i in range(len(names)):
        for j in range(i+1, len(names)):
            edges.append((names[i], names[j]))

G = nx.Graph()
G.add_edges_from(edges)

plt.figure(figsize=(12,12))
pos = nx.spring_layout(G, k=0.5)
nx.draw(G, pos, node_size=20, edge_color='gray', alpha=0.7)
plt.title("Réseau des co-auteurs")
plt.show()

# ------------------------------
# 7️⃣ Analyse laboratoires et pays
# ------------------------------
author_labs_full = author_labs.merge(labs, left_on='lab_id', right_on='id').merge(article_authors, left_on='author_id', right_on='author_id')
lab_counts = author_labs_full['lab_name'].value_counts().head(20)

plt.figure(figsize=(10,6))
sns.barplot(x=lab_counts.values, y=lab_counts.index)
plt.title("Top 20 laboratoires par nombre de publications")
plt.xlabel("Nombre de publications")
plt.ylabel("Laboratoire")
plt.show()

# Collaboration pays
author_labs_articles = author_labs_full.merge(articles[['id','title']], left_on='article_id', right_on='id')
country_edges = author_labs_articles.groupby('article_id')['country'].unique().tolist()
edges_country = []
for c_list in country_edges:
    for i in range(len(c_list)):
        for j in range(i+1, len(c_list)):
            edges_country.append((c_list[i], c_list[j]))

G_country = nx.Graph()
G_country.add_edges_from(edges_country)

plt.figure(figsize=(12,12))
pos = nx.spring_layout(G_country, k=0.5)
nx.draw(G_country, pos, node_size=100, node_color='skyblue', edge_color='gray', alpha=0.7, with_labels=True)
plt.title("Collaboration inter-pays")
plt.show()

# ------------------------------
# 8️⃣ Graphe interactif avec PyVis (optionnel)
# ------------------------------
net = Network(height="750px", width="100%", notebook=True)
net.from_nx(G)
net.show("coauthors_network.html")

# Fermeture de la connexion
conn.close()