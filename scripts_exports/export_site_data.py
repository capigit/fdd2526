from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
import csv
import json
import re
import sqlite3


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "bd" / "fusion_ieee.db"
SITE_DATA_DIR = PROJECT_ROOT / "site" / "public" / "data"
CLUSTER_FEATURES_PATH = PROJECT_ROOT / "analyse" / "prediction" / "features_prediction.csv"

MAX_ARTICLE_KEYWORDS = 12
MAX_GRAPH_NODES = 800
MAX_GRAPH_EDGES = 5000

CLUSTER_STOPWORDS = {
    "and", "for", "with", "from", "using", "based", "data", "models", "model",
    "systems", "system", "analysis", "approach", "method", "methods", "task",
    "study", "research", "performance", "learning", "machine", "deep",
    "artificial", "intelligence", "training", "classification", "feature",
    "features", "algorithms", "algorithm", "modeling", "extraction"
}

CLUSTER_THEME_RULES = [
    ({"medical", "healthcare", "diagnosis", "disease", "clinical", "cancer", "radiology", "imaging", "biomedical"}, "AI for health"),
    ({"language", "natural", "text", "llm", "large", "transformers", "sentiment", "speech"}, "Language models and NLP"),
    ({"wireless", "mobile", "communication", "resource", "scheduling"}, "Wireless communications"),
    ({"internet", "things", "iot", "smart"}, "IoT and smart systems"),
    ({"security", "privacy", "attack", "malware", "intrusion"}, "Security and privacy"),
    ({"reinforcement", "control", "decision", "q-learning"}, "Reinforcement and control"),
    ({"optimization", "scheduling", "resource"}, "Optimization and scheduling"),
    ({"prediction", "predictive", "forecasting", "accuracy"}, "Prediction and evaluation"),
    ({"image", "images", "vision", "segmentation", "detection", "recognition", "convolutional"}, "Computer vision"),
    ({"network", "networks", "neural", "cnn", "transfer"}, "Neural architectures"),
    ({"robot", "robots", "autonomous", "vehicle"}, "Robotics and autonomy"),
]


COUNTRY_COORDS = {
    "Argentina": [-38.4161, -63.6167],
    "Australia": [-25.2744, 133.7751],
    "Austria": [47.5162, 14.5501],
    "Belgium": [50.5039, 4.4699],
    "Brazil": [-14.2350, -51.9253],
    "Canada": [56.1304, -106.3468],
    "Chile": [-35.6751, -71.5430],
    "China": [35.8617, 104.1954],
    "Colombia": [4.5709, -74.2973],
    "Czech Republic": [49.8175, 15.4730],
    "Denmark": [56.2639, 9.5018],
    "Egypt": [26.8206, 30.8025],
    "Finland": [61.9241, 25.7482],
    "France": [46.603354, 1.888334],
    "Germany": [51.1657, 10.4515],
    "Greece": [39.0742, 21.8243],
    "Hong Kong": [22.3193, 114.1694],
    "Hungary": [47.1625, 19.5033],
    "India": [20.5937, 78.9629],
    "Indonesia": [-0.7893, 113.9213],
    "Iran": [32.4279, 53.6880],
    "Ireland": [53.4129, -8.2439],
    "Israel": [31.0461, 34.8516],
    "Italy": [41.8719, 12.5674],
    "Japan": [36.2048, 138.2529],
    "Jordan": [30.5852, 36.2384],
    "Malaysia": [4.2105, 101.9758],
    "Mexico": [23.6345, -102.5528],
    "Morocco": [31.7917, -7.0926],
    "Netherlands": [52.1326, 5.2913],
    "New Zealand": [-40.9006, 174.8860],
    "Norway": [60.4720, 8.4689],
    "Pakistan": [30.3753, 69.3451],
    "Poland": [51.9194, 19.1451],
    "Portugal": [39.3999, -8.2245],
    "Qatar": [25.3548, 51.1839],
    "Romania": [45.9432, 24.9668],
    "Saudi Arabia": [23.8859, 45.0792],
    "Singapore": [1.3521, 103.8198],
    "South Africa": [-30.5595, 22.9375],
    "South Korea": [35.9078, 127.7669],
    "Spain": [40.4637, -3.7492],
    "Sweden": [60.1282, 18.6435],
    "Switzerland": [46.8182, 8.2275],
    "Taiwan": [23.6978, 120.9605],
    "Thailand": [15.8700, 100.9925],
    "The Netherlands": [52.1326, 5.2913],
    "Tunisia": [33.8869, 9.5375],
    "Turkey": [38.9637, 35.2433],
    "UAE": [23.4241, 53.8478],
    "UK": [55.3781, -3.4360],
    "United Kingdom": [55.3781, -3.4360],
    "United States": [37.0902, -95.7129],
    "US": [37.0902, -95.7129],
    "USA": [37.0902, -95.7129],
    "Vietnam": [14.0583, 108.2772],
}


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def write_json(filename: str, data) -> None:
    SITE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    output_path = SITE_DATA_DIR / filename
    output_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"Wrote {output_path.relative_to(PROJECT_ROOT)}")


def parse_year(value: str | None) -> int | None:
    if not value:
        return None
    match = re.search(r"(19|20)\d{2}", str(value))
    return int(match.group(0)) if match else None


def excerpt(value: str | None, max_len: int = 300) -> str:
    if not value:
        return ""
    value = " ".join(str(value).split())
    if len(value) <= max_len:
        return value
    return value[: max_len - 1].rstrip() + "..."


def source_label(source: str | None) -> str:
    labels = {
        "ieee_ai_articles.db": "Artificial intelligence",
        "ieee_deep_learning.db": "Deep learning",
        "ieee_llm_articles.db": "LLM",
        "ieee_machine_learning.db": "Machine learning",
        "ieee_nlp.db": "NLP",
    }
    return labels.get(source or "", source or "Unknown")


def load_article_authors(conn: sqlite3.Connection):
    article_authors: dict[int, list[int]] = defaultdict(list)
    author_articles: dict[int, set[int]] = defaultdict(set)
    for row in conn.execute("SELECT article_id, author_id FROM article_authors"):
        article_id = int(row["article_id"])
        author_id = int(row["author_id"])
        article_authors[article_id].append(author_id)
        author_articles[author_id].add(article_id)
    return article_authors, author_articles


def load_keywords(conn: sqlite3.Connection):
    article_keywords: dict[int, list[str]] = defaultdict(list)
    keyword_counts = Counter()

    for row in conn.execute("SELECT article_id, keyword FROM keywords WHERE keyword IS NOT NULL AND keyword != ''"):
        keyword = str(row["keyword"]).strip()
        if not keyword:
            continue
        article_id = int(row["article_id"])
        if keyword not in article_keywords[article_id]:
            article_keywords[article_id].append(keyword)
        keyword_counts[keyword] += 1

    return article_keywords, keyword_counts


def load_author_countries(conn: sqlite3.Connection):
    author_country: dict[int, str] = {}
    author_lab_counts = Counter()

    query = """
    SELECT al.author_id, l.country
    FROM author_labs al
    JOIN labs l ON al.lab_id = l.id
    WHERE l.country IS NOT NULL AND l.country != '' AND l.country != 'Inconnu'
    ORDER BY al.author_id, al.lab_id
    """
    for row in conn.execute(query):
        author_id = int(row["author_id"])
        country = str(row["country"]).strip()
        author_lab_counts[(author_id, country)] += 1
        if author_id not in author_country:
            author_country[author_id] = country

    return author_country, author_lab_counts


def dominant_country(author_ids: list[int], author_country: dict[int, str]) -> str:
    countries = [author_country[author_id] for author_id in author_ids if author_id in author_country]
    if not countries:
        return "Unknown"
    return Counter(countries).most_common(1)[0][0]


def build_articles(conn, article_authors, article_keywords, author_country):
    articles = []
    timeline = Counter()
    sources = Counter()
    source_timeline = Counter()
    countries_articles: dict[str, set[int]] = defaultdict(set)

    query = """
    SELECT id, title, abstract, doi, date_publication, publisher, published_in, source
    FROM articles
    ORDER BY id
    """
    for row in conn.execute(query):
        article_id = int(row["id"])
        year = parse_year(row["date_publication"])
        source = row["source"] or "Unknown"
        authors = article_authors.get(article_id, [])
        country = dominant_country(authors, author_country)
        keywords = article_keywords.get(article_id, [])[:MAX_ARTICLE_KEYWORDS]

        if year:
            timeline[year] += 1
            source_timeline[(source, year)] += 1
        sources[source] += 1
        if country != "Unknown":
            countries_articles[country].add(article_id)

        articles.append({
            "id": article_id,
            "title": row["title"] or "Untitled",
            "abstract": excerpt(row["abstract"]),
            "doi": row["doi"] or "",
            "date": row["date_publication"] or "",
            "year": year,
            "publisher": row["publisher"] or "",
            "publishedIn": row["published_in"] or "",
            "source": source,
            "sourceLabel": source_label(source),
            "authorCount": len(set(authors)),
            "country": country,
            "keywords": keywords,
        })

    return articles, timeline, sources, source_timeline, countries_articles


def build_country_data(conn, countries_articles, author_country):
    country_author_counts = Counter(author_country.values())
    country_lab_counts = Counter()

    for row in conn.execute("SELECT country, COUNT(*) AS count FROM labs GROUP BY country"):
        country = row["country"] or "Unknown"
        country_lab_counts[country] = int(row["count"])

    countries = []
    for country, article_ids in countries_articles.items():
        lat_lng = COUNTRY_COORDS.get(country)
        countries.append({
            "country": country,
            "articles": len(article_ids),
            "authors": country_author_counts[country],
            "labs": country_lab_counts[country],
            "coordinates": lat_lng,
        })

    countries.sort(key=lambda item: item["articles"], reverse=True)
    return countries


def build_country_links(article_authors, author_country):
    link_counts = Counter()
    for author_ids in article_authors.values():
        countries = sorted({author_country[author_id] for author_id in author_ids if author_id in author_country})
        for country_a, country_b in combinations(countries, 2):
            link_counts[(country_a, country_b)] += 1

    links = [
        {"source": source, "target": target, "count": count}
        for (source, target), count in link_counts.items()
        if count >= 2 and source in COUNTRY_COORDS and target in COUNTRY_COORDS
    ]
    links.sort(key=lambda item: item["count"], reverse=True)
    return links[:80]


def build_top_lists(conn, keyword_counts, author_articles, author_country):
    top_keywords = [
        {"keyword": keyword, "count": count}
        for keyword, count in keyword_counts.most_common(40)
    ]

    author_names = {
        int(row["id"]): row["name"] or "Unknown"
        for row in conn.execute("SELECT id, name FROM authors")
    }

    top_authors = []
    for author_id, article_ids in sorted(author_articles.items(), key=lambda item: len(item[1]), reverse=True)[:50]:
        top_authors.append({
            "id": author_id,
            "name": author_names.get(author_id, "Unknown"),
            "articles": len(article_ids),
            "country": author_country.get(author_id, "Unknown"),
        })

    top_labs = []
    query = """
    SELECT l.id, l.lab_name, l.country, COUNT(DISTINCT al.author_id) AS authors
    FROM labs l
    LEFT JOIN author_labs al ON l.id = al.lab_id
    GROUP BY l.id, l.lab_name, l.country
    ORDER BY authors DESC
    LIMIT 50
    """
    for row in conn.execute(query):
        top_labs.append({
            "id": int(row["id"]),
            "name": row["lab_name"] or "Unknown",
            "country": row["country"] or "Unknown",
            "authors": int(row["authors"]),
        })

    return top_keywords, top_authors, top_labs


def build_graph(conn, article_authors, author_articles, author_country):
    author_names = {
        int(row["id"]): row["name"] or "Unknown"
        for row in conn.execute("SELECT id, name FROM authors")
    }

    edge_counts = Counter()
    degree = Counter()

    for author_ids in article_authors.values():
        unique_authors = sorted(set(author_ids))
        for author_a, author_b in combinations(unique_authors, 2):
            edge_counts[(author_a, author_b)] += 1
            degree[author_a] += 1
            degree[author_b] += 1

    selected_ids = {
        author_id
        for author_id, _ in degree.most_common(MAX_GRAPH_NODES)
    }

    nodes = []
    for author_id in selected_ids:
        nodes.append({
            "id": str(author_id),
            "label": author_names.get(author_id, "Unknown"),
            "degree": degree[author_id],
            "articles": len(author_articles.get(author_id, [])),
            "country": author_country.get(author_id, "Unknown"),
        })

    edges = []
    for (source, target), weight in edge_counts.most_common():
        if source in selected_ids and target in selected_ids:
            edges.append({
                "id": f"{source}-{target}",
                "source": str(source),
                "target": str(target),
                "weight": weight,
            })
        if len(edges) >= MAX_GRAPH_EDGES:
            break

    nodes.sort(key=lambda item: item["degree"], reverse=True)
    return {"nodes": nodes, "edges": edges}


def build_cluster_summary():
    if not CLUSTER_FEATURES_PATH.exists():
        return []

    clusters: dict[str, dict] = {}
    with CLUSTER_FEATURES_PATH.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cluster = str(row.get("cluster", "Unknown"))
            bucket = clusters.setdefault(cluster, {
                "cluster": cluster,
                "size": 0,
                "authorsTotal": 0.0,
                "countries": Counter(),
                "terms": Counter(),
            })
            bucket["size"] += 1
            try:
                bucket["authorsTotal"] += float(row.get("nb_auteurs") or 0)
            except ValueError:
                pass
            country = row.get("country") or "Unknown"
            bucket["countries"][country] += 1
            for term in str(row.get("keyword") or "").split():
                term = term.lower().strip(".,;:()[]{}")
                if len(term) > 2:
                    bucket["terms"][term] += 1

    summaries = []
    for bucket in clusters.values():
        top_terms = [
            {"term": term, "count": count}
            for term, count in bucket["terms"].most_common(16)
        ]
        signal_terms = [
            item["term"]
            for item in top_terms
            if item["term"] not in CLUSTER_STOPWORDS
        ]
        term_set = set(signal_terms[:12]) | {item["term"] for item in top_terms[:12]}
        label = infer_cluster_label(term_set, signal_terms)
        summaries.append({
            "cluster": bucket["cluster"],
            "label": label,
            "description": cluster_description(label, bucket["size"], bucket["countries"]),
            "size": bucket["size"],
            "avgAuthors": round(bucket["authorsTotal"] / bucket["size"], 2) if bucket["size"] else 0,
            "topCountry": bucket["countries"].most_common(1)[0][0] if bucket["countries"] else "Unknown",
            "topTerms": top_terms[:8],
            "signalTerms": signal_terms[:8],
        })

    summaries.sort(key=lambda item: item["size"], reverse=True)
    return summaries


def build_insights(timeline_data, source_data, top_keywords, countries_payload, clusters):
    insights = []

    if timeline_data:
        peak_year = max(timeline_data, key=lambda item: item["count"])
        first_year = timeline_data[0]
        last_year = timeline_data[-1]
        growth = last_year["count"] - first_year["count"]
        insights.append({
            "title": "Acceleration temporelle",
            "value": str(peak_year["year"]),
            "detail": f"Annee la plus dense avec {peak_year['count']} articles. Le corpus passe de {first_year['count']} articles en {first_year['year']} a {last_year['count']} en {last_year['year']}.",
            "tone": "teal",
            "metric": growth,
        })

    if source_data:
        dominant_source = max(source_data, key=lambda item: item["count"])
        total = sum(item["count"] for item in source_data) or 1
        share = dominant_source["count"] / total * 100
        insights.append({
            "title": "Source dominante",
            "value": dominant_source["label"],
            "detail": f"{dominant_source['count']} articles, soit {share:.1f}% du corpus fusionne.",
            "tone": "cobalt",
            "metric": dominant_source["count"],
        })

    if top_keywords:
        keyword = top_keywords[0]
        insights.append({
            "title": "Theme lexical majeur",
            "value": keyword["keyword"],
            "detail": f"Mot-cle le plus frequent avec {keyword['count']} occurrences dans les metadonnees IEEE.",
            "tone": "amber",
            "metric": keyword["count"],
        })

    countries = countries_payload["countries"]
    if countries:
        country = countries[0]
        insights.append({
            "title": "Pole geographique",
            "value": country["country"],
            "detail": f"{country['articles']} articles, {country['authors']} auteurs et {country['labs']} laboratoires identifies.",
            "tone": "rosewood",
            "metric": country["articles"],
        })

    links = countries_payload["links"]
    if links:
        link = links[0]
        insights.append({
            "title": "Collaboration bilaterale",
            "value": f"{link['source']} - {link['target']}",
            "detail": f"Lien international le plus visible avec {link['count']} co-publications detectees.",
            "tone": "teal",
            "metric": link["count"],
        })

    if clusters:
        cluster = clusters[0]
        insights.append({
            "title": "Grand cluster thematique",
            "value": cluster["label"],
            "detail": f"Cluster {cluster['cluster']} avec {cluster['size']} articles. Termes signaux : {', '.join(cluster['signalTerms'][:4])}.",
            "tone": "cobalt",
            "metric": cluster["size"],
        })

    return insights


def infer_cluster_label(term_set: set[str], signal_terms: list[str]) -> str:
    for rule_terms, label in CLUSTER_THEME_RULES:
        if len(term_set.intersection(rule_terms)) >= 1:
            return label

    if len(signal_terms) >= 2:
        return f"{signal_terms[0].title()} and {signal_terms[1].title()}"
    if signal_terms:
        return signal_terms[0].title()
    return "General AI methods"


def cluster_description(label: str, size: int, countries: Counter) -> str:
    country = countries.most_common(1)[0][0] if countries else "Unknown"
    return f"{label} regroupe {size} articles, avec une presence dominante de {country}."


def main() -> None:
    with connect() as conn:
        article_authors, author_articles = load_article_authors(conn)
        article_keywords, keyword_counts = load_keywords(conn)
        author_country, _ = load_author_countries(conn)

        articles, timeline, sources, source_timeline, countries_articles = build_articles(
            conn,
            article_authors,
            article_keywords,
            author_country,
        )
        countries = build_country_data(conn, countries_articles, author_country)
        country_links = build_country_links(article_authors, author_country)
        top_keywords, top_authors, top_labs = build_top_lists(
            conn,
            keyword_counts,
            author_articles,
            author_country,
        )
        graph = build_graph(conn, article_authors, author_articles, author_country)

        counts = {
            table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            for table in ["articles", "authors", "labs", "keywords", "article_authors", "author_labs"]
        }

    summary = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "counts": counts,
        "coverage": {
            "articlesWithKeywords": sum(1 for item in articles if item["keywords"]),
            "articlesWithCountry": sum(1 for item in articles if item["country"] != "Unknown"),
            "graphNodes": len(graph["nodes"]),
            "graphEdges": len(graph["edges"]),
        },
    }

    timeline_data = [
        {"year": year, "count": count}
        for year, count in sorted(timeline.items())
    ]
    source_data = [
        {"source": source, "label": source_label(source), "count": count}
        for source, count in sources.most_common()
    ]
    source_timeline_data = [
        {
            "source": source,
            "label": source_label(source),
            "year": year,
            "count": count,
        }
        for (source, year), count in sorted(source_timeline.items(), key=lambda item: (item[0][1], item[0][0]))
    ]

    countries_payload = {"countries": countries, "links": country_links}
    clusters = build_cluster_summary()
    insights = build_insights(timeline_data, source_data, top_keywords, countries_payload, clusters)

    write_json("summary.json", summary)
    write_json("articles.json", articles)
    write_json("timeline.json", timeline_data)
    write_json("sources.json", source_data)
    write_json("source_timeline.json", source_timeline_data)
    write_json("keywords.json", top_keywords)
    write_json("authors.json", top_authors)
    write_json("labs.json", top_labs)
    write_json("countries.json", countries_payload)
    write_json("graph.json", graph)
    write_json("clusters.json", clusters)
    write_json("insights.json", insights)


if __name__ == "__main__":
    main()
