import pandas as pd
import sqlite3
import networkx as nx
from pyvis.network import Network
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from pathlib import Path

DB_PATH = "bd/fusion_ieee.db"
OUTPUT_DIR = Path("outputs/collaboration")
GEXF_PATH = OUTPUT_DIR / "graphe_collaboration.gexf"
HTML_PATH = OUTPUT_DIR / "graphe_collaboration.html"

def load_data():
    conn = sqlite3.connect(DB_PATH)
    df_aa = pd.read_sql_query("SELECT * FROM article_authors", conn)
    df_authors = pd.read_sql_query("SELECT * FROM authors", conn)
    conn.close()
    return df_aa, df_authors

def build_graph(df_aa, df_authors):
    G = nx.Graph()

    # Ajouter les auteurs comme nœuds
    for _, row in df_authors.iterrows():
        G.add_node(row["id"], name=row["name"])

    # Ajouter les arêtes (co-auteurs)
    grouped = df_aa.groupby("article_id")["author_id"].apply(list)

    for authors in grouped:
        if len(authors) > 1:
            for i in range(len(authors)):
                for j in range(i+1, len(authors)):
                    a1, a2 = authors[i], authors[j]
                    if G.has_edge(a1, a2):
                        G[a1][a2]['weight'] += 1
                    else:
                        G.add_edge(a1, a2, weight=1)

    return G

def generate_html_graph(G, output_path=HTML_PATH):
    """
    Génère une visualisation HTML interactive du graphe avec des couleurs
    """
    # Créer le réseau Pyvis
    net = Network(
        height="900px",
        width="100%",
        bgcolor="#ffffff",
        font_color="#333333",
        notebook=False
    )
    
    # Configurer les options de physique pour un meilleur rendu
    net.set_options("""
    {
      "nodes": {
        "font": {
          "size": 14,
          "face": "Arial"
        },
        "borderWidth": 2,
        "borderWidthSelected": 3
      },
      "edges": {
        "color": {
          "inherit": false,
          "color": "#cccccc",
          "highlight": "#ff6b6b",
          "hover": "#ff6b6b"
        },
        "smooth": {
          "enabled": true,
          "type": "continuous"
        }
      },
      "physics": {
        "forceAtlas2Based": {
          "gravitationalConstant": -50,
          "centralGravity": 0.01,
          "springLength": 200,
          "springConstant": 0.08
        },
        "maxVelocity": 50,
        "solver": "forceAtlas2Based",
        "timestep": 0.35,
        "stabilization": {
          "enabled": true,
          "iterations": 150
        }
      },
      "interaction": {
        "hover": true,
        "tooltipDelay": 100,
        "navigationButtons": true,
        "keyboard": true
      }
    }
    """)
    
    # Calculer le degré de chaque nœud pour la taille et la couleur
    degrees = dict(G.degree())
    max_degree = max(degrees.values()) if degrees else 1
    
    # Créer une palette de couleurs basée sur le degré
    colormap = plt.cm.get_cmap('viridis')
    
    # Ajouter les nœuds avec des couleurs et tailles basées sur leur degré
    for node in G.nodes():
        degree = degrees[node]
        # Taille proportionnelle au degré (entre 10 et 50)
        size = 10 + (degree / max_degree) * 40
        
        # Couleur basée sur le degré
        color_value = degree / max_degree
        rgb = colormap(color_value)
        hex_color = mcolors.rgb2hex(rgb)
        
        # Nom de l'auteur
        name = G.nodes[node].get('name', f'Author {node}')
        
        # Titre avec informations
        title = f"<b>{name}</b><br>Collaborations: {degree}"
        
        net.add_node(
            node,
            label=name,
            title=title,
            size=size,
            color=hex_color,
            borderWidth=2,
            borderWidthSelected=4
        )
    
    # Ajouter les arêtes avec des poids
    for edge in G.edges(data=True):
        source, target, data = edge
        weight = data.get('weight', 1)
        
        # Épaisseur de l'arête basée sur le poids
        width = 1 + (weight * 0.5)
        
        # Titre pour montrer le nombre de collaborations
        title = f"Collaborations: {weight}"
        
        net.add_edge(
            source,
            target,
            value=width,
            title=title
        )
    
    # Sauvegarder le fichier HTML
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    net.save_graph(str(output_path))
    print(f"Graphe HTML genere : {output_path}")
    
    return output_path

def generate_statistics(G):
    """
    Génère des statistiques sur le graphe
    """
    print("\n" + "="*60)
    print("STATISTIQUES DU GRAPHE DE COLLABORATION")
    print("="*60)
    print(f"Nombre d'auteurs (nœuds)     : {G.number_of_nodes()}")
    print(f"Nombre de collaborations      : {G.number_of_edges()}")
    
    if G.number_of_nodes() > 0:
        degrees = dict(G.degree())
        avg_degree = sum(degrees.values()) / len(degrees)
        print(f"Degré moyen                   : {avg_degree:.2f}")
        
        # Trouver les auteurs les plus collaboratifs
        top_collaborators = sorted(degrees.items(), key=lambda x: x[1], reverse=True)[:5]
        print("\nTop 5 auteurs les plus collaboratifs:")
        for author_id, degree in top_collaborators:
            name = G.nodes[author_id].get('name', f'Author {author_id}')
            print(f"  - {name}: {degree} collaborations")
        
        # Composantes connexes
        num_components = nx.number_connected_components(G)
        print(f"\nNombre de communautés         : {num_components}")
        
        if num_components > 0:
            largest_component = max(nx.connected_components(G), key=len)
            print(f"Taille de la plus grande      : {len(largest_component)} auteurs")
    
    print("="*60 + "\n")

if __name__ == "__main__":
    print("Chargement des données...")
    df_aa, df_authors = load_data()
    
    print("Construction du graphe...")
    G = build_graph(df_aa, df_authors)
    
    # Générer les statistiques
    generate_statistics(G)
    
    # Exporter en GEXF (format original)
    print("Export GEXF...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    nx.write_gexf(G, GEXF_PATH)
    print(f"Export GEXF : {GEXF_PATH}")
    
    # Générer la visualisation HTML
    print("\nGénération de la visualisation HTML...")
    html_path = generate_html_graph(G)
    
    print("\nTermine ! Ouvrez le fichier dans votre navigateur :")
    print(f"   {html_path}")
