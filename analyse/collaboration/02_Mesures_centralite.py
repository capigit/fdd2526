import networkx as nx
import pandas as pd

# Charger le graphe construit
G = nx.read_gexf("analyse/collaboration/graphe_collaboration.gexf")
print(f"Graphe chargé : {G.number_of_nodes()} nœuds, {G.number_of_edges()} arêtes")

# Degree (nombre de connexions par auteur)
degree_dict = dict(G.degree())
print("Degree calculé.")

# PageRank (importance relative des auteurs)
pagerank_dict = nx.pagerank(G, alpha=0.85)
print("PageRank calculé.")

# Betweenness approximation (optionnel mais rapide)
# k = 1000 : échantillon de 1000 nœuds pour accélérer le calcul
betweenness_dict = nx.betweenness_centrality(G, k=1000, seed=42, normalized=True)
print("Betweenness approximative calculée.")

# Closeness centrality
closeness_dict = nx.closeness_centrality(G)
print("Closeness calculée.")

# Fusion des mesures dans un DataFrame
df_centrality = pd.DataFrame({
    "author_id": list(G.nodes),
    "degree": [degree_dict[n] for n in G.nodes],
    "pagerank": [pagerank_dict[n] for n in G.nodes],
    "betweenness": [betweenness_dict[n] for n in G.nodes],
    "closeness": [closeness_dict[n] for n in G.nodes],
})

# Export CSV
df_centrality.to_csv("analyse/collaboration/centrality.csv", index=False)
print("Mesures de centralité exportées : analyse/collaboration/centrality.csv")