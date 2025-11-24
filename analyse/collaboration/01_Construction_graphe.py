import pandas as pd
import sqlite3
import networkx as nx

DB_PATH = "bd/fusion_ieee.db"

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

if __name__ == "__main__":
    df_aa, df_authors = load_data()
    G = build_graph(df_aa, df_authors)

    nx.write_gexf(G, "analyse/collaboration/graphe_collaboration.gexf")

    print("Graphe construit :", G.number_of_nodes(), "nœuds,", G.number_of_edges(), "arêtes.")
    print("Export : analyse/collaboration/graphe_collaboration.gexf")