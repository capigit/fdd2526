import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
import community as community_louvain
import matplotlib.colors as mcolors
from pathlib import Path

# Chemin des fichiers
OUTPUT_DIR = Path("outputs/collaboration")
G_path = OUTPUT_DIR / "graphe_collaboration.gexf"
comm_path = OUTPUT_DIR / "communities.csv"
output_png = OUTPUT_DIR / "graph_communautes.png"

# Chargement du graphe
G = nx.read_gexf(G_path)
print(f"Graphe chargé : {G.number_of_nodes()} nœuds, {G.number_of_edges()} arêtes")

# Chargement des communautés
df_comm = pd.read_csv(comm_path)
# df_comm doit contenir : author_id, community
comm_dict = dict(zip(df_comm['author_id'], df_comm['community']))

# Ajouter la communauté comme attribut
nx.set_node_attributes(G, comm_dict, name='community')

# Visualisation
# On va limiter la visualisation aux 1000 auteurs les plus connectés pour plus de clarté
top_nodes = sorted(G.degree, key=lambda x: x[1], reverse=True)[:1000]
G_sub = G.subgraph([n for n, d in top_nodes])

# Couleur par communauté
communities = [G_sub.nodes[n].get('community', 0) for n in G_sub.nodes()]
unique_comms = list(set(communities))
color_list = list(mcolors.CSS4_COLORS.values())
colors = [color_list[i % len(color_list)] for i in communities]

plt.figure(figsize=(15, 12))
pos = nx.spring_layout(G_sub, seed=42, k=0.1)  # Force-directed layout

nx.draw_networkx_nodes(G_sub, pos, node_size=50, node_color=colors, alpha=0.8)
nx.draw_networkx_edges(G_sub, pos, alpha=0.1)
plt.title("Visualisation des communautés (top 1000 auteurs les plus connectés)")
plt.axis('off')
plt.tight_layout()
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
plt.savefig(output_png, dpi=300)
plt.show()

print(f"Visualisation exportée : {output_png}")
