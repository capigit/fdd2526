import type {
  Article,
  AuthorPoint,
  ClusterSummary,
  CountriesPayload,
  GraphPayload,
  Insight,
  KeywordPoint,
  LabPoint,
  SiteData,
  SourcePoint,
  SourceYearPoint,
  Summary,
  TimelinePoint
} from "./types";

async function getJson<T>(path: string): Promise<T> {
  const response = await fetch(`${import.meta.env.BASE_URL}data/${path}`);
  if (!response.ok) {
    throw new Error(`Impossible de charger ${path}`);
  }
  return response.json() as Promise<T>;
}

export async function loadCoreData(): Promise<SiteData> {
  const [
    summary,
    timeline,
    sources,
    sourceTimeline,
    keywords,
    authors,
    labs,
    countries,
    clusters,
    insights
  ] = await Promise.all([
    getJson<Summary>("summary.json"),
    getJson<TimelinePoint[]>("timeline.json"),
    getJson<SourcePoint[]>("sources.json"),
    getJson<SourceYearPoint[]>("source_timeline.json"),
    getJson<KeywordPoint[]>("keywords.json"),
    getJson<AuthorPoint[]>("authors.json"),
    getJson<LabPoint[]>("labs.json"),
    getJson<CountriesPayload>("countries.json"),
    getJson<ClusterSummary[]>("clusters.json"),
    getJson<Insight[]>("insights.json")
  ]);

  return { summary, timeline, sources, sourceTimeline, keywords, authors, labs, countries, clusters, insights };
}

export function loadArticles(): Promise<Article[]> {
  return getJson<Article[]>("articles.json");
}

export function loadGraph(): Promise<GraphPayload> {
  return getJson<GraphPayload>("graph.json");
}
