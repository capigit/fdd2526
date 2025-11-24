import networkx as nx
import pandas as pd
import community as community_louvain  # python-louvain
import os

# Chemin du graphe construit précédemment
graph_path = "analyse/collaboration/graphe_collaboration.gexf"

# Vérifier que le fichier existe
if not os.path.exists(graph_path):
    raise FileNotFoundError(f"Le graphe {graph_path} est introuvable. Exécutez d'abord 01_Construction_graphe.py")

# Charger le graphe
G = nx.read_gexf(graph_path)
print(f"Graphe chargé : {G.number_of_nodes()} nœuds, {G.number_of_edges()} arêtes")

# Détection des communautés avec Louvain
print("Détection des communautés avec Louvain...")
partition = community_louvain.best_partition(G)

# Transformer en DataFrame
df_communities = pd.DataFrame(list(partition.items()), columns=['author_id', 'community'])
print(f"Nombre de communautés détectées : {df_communities['community'].nunique()}")
print(f"Taille moyenne d'une communauté : {df_communities.groupby('community').size().mean():.2f} auteurs")

# Export CSV
output_path = "analyse/collaboration/communities.csv"
df_communities.to_csv(output_path, index=False)
print(f"Communautés exportées : {output_path}")
