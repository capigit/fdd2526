export type Summary = {
  generatedAt: string;
  counts: Record<string, number>;
  coverage: {
    articlesWithKeywords: number;
    articlesWithCountry: number;
    graphNodes: number;
    graphEdges: number;
  };
};

export type Article = {
  id: number;
  title: string;
  abstract: string;
  doi: string;
  date: string;
  year: number | null;
  publisher: string;
  publishedIn: string;
  source: string;
  sourceLabel: string;
  authorCount: number;
  country: string;
  keywords: string[];
};

export type TimelinePoint = {
  year: number;
  count: number;
};

export type SourcePoint = {
  source: string;
  label: string;
  count: number;
};

export type SourceYearPoint = {
  source: string;
  label: string;
  year: number;
  count: number;
};

export type KeywordPoint = {
  keyword: string;
  count: number;
};

export type AuthorPoint = {
  id: number;
  name: string;
  articles: number;
  country: string;
};

export type LabPoint = {
  id: number;
  name: string;
  country: string;
  authors: number;
};

export type CountryPoint = {
  country: string;
  articles: number;
  authors: number;
  labs: number;
  coordinates: [number, number] | null;
};

export type CountryLink = {
  source: string;
  target: string;
  count: number;
};

export type CountriesPayload = {
  countries: CountryPoint[];
  links: CountryLink[];
};

export type GraphPayload = {
  nodes: Array<{
    id: string;
    label: string;
    degree: number;
    articles: number;
    country: string;
  }>;
  edges: Array<{
    id: string;
    source: string;
    target: string;
    weight: number;
  }>;
};

export type ClusterSummary = {
  cluster: string;
  label: string;
  description: string;
  size: number;
  avgAuthors: number;
  topCountry: string;
  topTerms: Array<{ term: string; count: number }>;
  signalTerms: string[];
};

export type Insight = {
  title: string;
  value: string;
  detail: string;
  tone: "teal" | "cobalt" | "amber" | "rosewood";
  metric: number;
};

export type SiteData = {
  summary: Summary;
  timeline: TimelinePoint[];
  sources: SourcePoint[];
  sourceTimeline: SourceYearPoint[];
  keywords: KeywordPoint[];
  authors: AuthorPoint[];
  labs: LabPoint[];
  countries: CountriesPayload;
  clusters: ClusterSummary[];
  insights: Insight[];
};
