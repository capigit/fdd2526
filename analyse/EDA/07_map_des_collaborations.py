from collections import Counter
from html import escape
from pathlib import Path
import itertools
import sqlite3

import folium
from folium import plugins
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "bd" / "fusion_ieee.db"
OUTPUT_PATH = PROJECT_ROOT / "outputs" / "collaboration" / "carte_vraies_collaborations.html"


COUNTRY_COORDS = {
    "Argentina": (-38.4161, -63.6167),
    "Australia": (-25.2744, 133.7751),
    "Austria": (47.5162, 14.5501),
    "Belgium": (50.5039, 4.4699),
    "Brazil": (-14.2350, -51.9253),
    "Canada": (56.1304, -106.3468),
    "Chile": (-35.6751, -71.5430),
    "China": (35.8617, 104.1954),
    "Colombia": (4.5709, -74.2973),
    "Czech Republic": (49.8175, 15.4730),
    "Denmark": (56.2639, 9.5018),
    "Egypt": (26.8206, 30.8025),
    "Finland": (61.9241, 25.7482),
    "France": (46.603354, 1.888334),
    "Germany": (51.1657, 10.4515),
    "Greece": (39.0742, 21.8243),
    "Hungary": (47.1625, 19.5033),
    "India": (20.5937, 78.9629),
    "Indonesia": (-0.7893, 113.9213),
    "Iran": (32.4279, 53.6880),
    "Ireland": (53.4129, -8.2439),
    "Israel": (31.0461, 34.8516),
    "Italy": (41.8719, 12.5674),
    "Japan": (36.2048, 138.2529),
    "Malaysia": (4.2105, 101.9758),
    "Mexico": (23.6345, -102.5528),
    "Morocco": (31.7917, -7.0926),
    "Netherlands": (52.1326, 5.2913),
    "New Zealand": (-40.9006, 174.8860),
    "Norway": (60.4720, 8.4689),
    "Pakistan": (30.3753, 69.3451),
    "Poland": (51.9194, 19.1451),
    "Portugal": (39.3999, -8.2245),
    "Romania": (45.9432, 24.9668),
    "Saudi Arabia": (23.8859, 45.0792),
    "Singapore": (1.3521, 103.8198),
    "South Africa": (-30.5595, 22.9375),
    "South Korea": (35.9078, 127.7669),
    "Spain": (40.4637, -3.7492),
    "Sweden": (60.1282, 18.6435),
    "Switzerland": (46.8182, 8.2275),
    "Taiwan": (23.6978, 120.9605),
    "Thailand": (15.8700, 100.9925),
    "The Netherlands": (52.1326, 5.2913),
    "Tunisia": (33.8869, 9.5375),
    "Turkey": (38.9637, 35.2433),
    "UAE": (23.4241, 53.8478),
    "UK": (55.3781, -3.4360),
    "United Kingdom": (55.3781, -3.4360),
    "United States": (37.0902, -95.7129),
    "US": (37.0902, -95.7129),
    "USA": (37.0902, -95.7129),
    "Vietnam": (14.0583, 108.2772),
}


def load_author_countries():
    query = """
    WITH author_main_lab AS (
        SELECT
            al.author_id,
            MIN(al.lab_id) AS main_lab_id
        FROM author_labs al
        GROUP BY al.author_id
    )
    SELECT DISTINCT
        a.id AS article_id,
        a.title,
        au.id AS author_id,
        au.name AS author_name,
        l.country,
        l.lab_name
    FROM articles a
    JOIN article_authors aa ON a.id = aa.article_id
    JOIN authors au ON aa.author_id = au.id
    JOIN author_main_lab aml ON au.id = aml.author_id
    JOIN labs l ON aml.main_lab_id = l.id
    WHERE l.country IS NOT NULL
      AND l.country != ''
      AND l.country != 'Inconnu'
    ORDER BY a.id, au.id
    """
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(query, conn)


def compute_collaborations(df):
    collaborations = []
    collab_details = []

    for article_id, group in df.groupby("article_id"):
        author_countries = group[["author_id", "country"]].drop_duplicates()
        countries = sorted(author_countries["country"].dropna().unique())

        if len(countries) <= 1:
            continue

        collaborations.extend(itertools.combinations(countries, 2))
        collab_details.append({
            "article_id": article_id,
            "title": group["title"].iloc[0],
            "nb_countries": len(countries),
            "countries": ", ".join(countries),
            "nb_authors": len(group),
        })

    collab_counts = Counter(collaborations)
    if not collab_counts:
        return pd.DataFrame(columns=["country1", "country2", "count"]), pd.DataFrame(collab_details)

    collab_df = pd.DataFrame(collab_counts.items(), columns=["pair", "count"])
    collab_df[["country1", "country2"]] = pd.DataFrame(
        collab_df["pair"].tolist(),
        index=collab_df.index,
    )
    return (
        collab_df[["country1", "country2", "count"]].sort_values("count", ascending=False),
        pd.DataFrame(collab_details),
    )


def add_collaboration_lines(map_obj, collab_df):
    collab_significant = collab_df[collab_df["count"] >= 2].head(40)
    print(f"Ajout de {len(collab_significant)} collaborations significatives...")

    for _, row in collab_significant.iterrows():
        country1 = row["country1"]
        country2 = row["country2"]
        if country1 not in COUNTRY_COORDS or country2 not in COUNTRY_COORDS:
            continue

        lat1, lon1 = COUNTRY_COORDS[country1]
        lat2, lon2 = COUNTRY_COORDS[country2]
        count = int(row["count"])
        weight = min(2 + count / 1.5, 10)

        if count >= 10:
            color = "#c41e3a"
            opacity = 0.8
        elif count >= 5:
            color = "#ff6b35"
            opacity = 0.7
        else:
            color = "#4ecdc4"
            opacity = 0.6

        folium.PolyLine(
            locations=[[lat1, lon1], [lat2, lon2]],
            color=color,
            weight=weight,
            opacity=opacity,
            popup=f"<b>{escape(country1)} <-> {escape(country2)}</b><br>{count} collaborations",
            tooltip=f"{count} collaborations",
        ).add_to(map_obj)


def add_country_markers(map_obj, df, country_counts):
    for _, row in country_counts.iterrows():
        country = row["country"]
        if country not in COUNTRY_COORDS:
            continue

        nb_pubs = int(row["nb_publications"])
        lat, lon = COUNTRY_COORDS[country]
        radius = min(5 + nb_pubs / 10, 25)

        if nb_pubs >= 100:
            color, fill_color = "#b22222", "#ff4444"
        elif nb_pubs >= 50:
            color, fill_color = "#ff8c00", "#ffaa44"
        elif nb_pubs >= 20:
            color, fill_color = "#4169e1", "#6495ed"
        else:
            color, fill_color = "#20b2aa", "#7fffd4"

        country_df = df[df["country"] == country]
        popup_html = f"""
        <div style="font-family: Arial; width: 200px;">
            <h4 style="margin: 0; color: {color};">{escape(country)}</h4>
            <hr style="margin: 5px 0;">
            <b>Publications:</b> {nb_pubs}<br>
            <b>Auteurs:</b> {country_df['author_id'].nunique()}<br>
            <b>Labs:</b> {country_df['lab_name'].nunique()}
        </div>
        """

        folium.CircleMarker(
            location=[lat, lon],
            radius=radius,
            popup=folium.Popup(popup_html, max_width=220),
            tooltip=f"{country}: {nb_pubs} pubs",
            color=color,
            fillColor=fill_color,
            fillOpacity=0.8,
            weight=2,
        ).add_to(map_obj)

        if nb_pubs >= 30:
            folium.Marker(
                location=[lat, lon],
                icon=folium.DivIcon(html=f"""
                    <div style="font-size: 11pt; color: black; font-weight: bold;
                                text-shadow: -1px -1px 0 white, 1px -1px 0 white,
                                -1px 1px 0 white, 1px 1px 0 white;">
                        {escape(country)}
                    </div>
                """),
            ).add_to(map_obj)


def add_layout(map_obj):
    legend_html = """
    <div style="position: fixed;
                bottom: 50px; right: 50px; width: 280px;
                background-color: white; z-index:9999; font-size:13px;
                border:2px solid grey; border-radius: 5px; padding: 10px;
                box-shadow: 3px 3px 10px rgba(0,0,0,0.3);">
        <h4 style="margin-top:0;">Collaborations IA/ML</h4>
        <p style="margin: 5px 0;"><b>Cercles:</b> nb publications</p>
        <p style="margin: 5px 0;"><b>Lignes:</b> collaborations reelles</p>
        <hr style="margin: 10px 0;">
        <p style="margin: 5px 0;">
            <span style="color: #c41e3a;">---</span> >=10 collabs<br>
            <span style="color: #ff6b35;">---</span> 5-9 collabs<br>
            <span style="color: #4ecdc4;">---</span> 2-4 collabs
        </p>
    </div>
    """
    map_obj.get_root().html.add_child(folium.Element(legend_html))

    title_html = """
    <div style="position: fixed;
                top: 10px; left: 50px; width: 550px;
                background-color: white; z-index:9999;
                border:2px solid grey; border-radius: 5px; padding: 10px;">
        <h3 style="margin:0; text-align: center; color: #2c3e50;">
            Vraies Collaborations Internationales en IA/ML
            <br><span style="font-size: 14px; color: #7f8c8d;">(2018-2025)</span>
        </h3>
    </div>
    """
    map_obj.get_root().html.add_child(folium.Element(title_html))
    plugins.Fullscreen().add_to(map_obj)


def main():
    df = load_author_countries()
    print(f"{len(df)} lignes extraites (1 lab par auteur)")
    print(f"Pays uniques : {df['country'].nunique()}")
    print(f"Articles uniques : {df['article_id'].nunique()}")
    print(f"Auteurs uniques : {df['author_id'].nunique()}")

    duplicate_check = df.groupby(["article_id", "author_id"]).size()
    if (duplicate_check > 1).any():
        print("WARNING: Il y a encore des doublons auteur-article.")
    else:
        print("Verification OK: 1 seul pays par auteur par article.")

    country_counts = (
        df.groupby("country")["article_id"]
        .nunique()
        .reset_index(name="nb_publications")
        .sort_values("nb_publications", ascending=False)
    )

    print("\nTop 10 pays par nombre de publications :")
    print(country_counts.head(10))

    print("\nIdentification des vraies collaborations internationales...")
    collab_df, collab_details_df = compute_collaborations(df)
    print(f"{len(collab_details_df)} articles en collaboration internationale detectes")
    print(f"{len(collab_df)} paires de collaboration uniques")

    print("\nTop 15 collaborations bilaterales :")
    print(collab_df.head(15))

    if not collab_details_df.empty:
        print("\nExemples d'articles en collaboration internationale :")
        print(
            collab_details_df
            .sort_values("nb_countries", ascending=False)
            .head(5)[["article_id", "nb_countries", "countries", "nb_authors"]]
        )

    map_obj = folium.Map(
        location=[30, 10],
        zoom_start=2,
        tiles="CartoDB positron",
        width="100%",
        height="100%",
    )
    add_collaboration_lines(map_obj, collab_df)
    add_country_markers(map_obj, df, country_counts)
    add_layout(map_obj)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    map_obj.save(OUTPUT_PATH)
    print(f"\nCarte sauvegardee : {OUTPUT_PATH}")

    total_articles = df["article_id"].nunique()
    collab_articles = len(collab_details_df)
    taux_collab = (collab_articles / total_articles * 100) if total_articles > 0 else 0

    print("\n" + "=" * 70)
    print("STATISTIQUES CORRIGEES")
    print("=" * 70)
    print(f"Total articles: {total_articles}")
    print(f"Pays impliques: {df['country'].nunique()}")
    print(f"Auteurs uniques: {df['author_id'].nunique()}")
    print(f"Laboratoires: {df['lab_name'].nunique()}")
    print(f"Articles en collaboration internationale: {collab_articles} ({taux_collab:.1f}%)")
    print(f"Paires de collaboration: {len(collab_df)}")

    print("\nTOP 5 PAYS")
    for _, row in country_counts.head(5).iterrows():
        print(f"{row['country']:25s} {int(row['nb_publications']):4d} publications")

    print("\nTOP 10 COLLABORATIONS BILATERALES")
    for _, row in collab_df.head(10).iterrows():
        print(f"{row['country1']:20s} <-> {row['country2']:20s} : {int(row['count']):3d} articles")

    print("\nCarte interactive generee avec succes.")


if __name__ == "__main__":
    main()
