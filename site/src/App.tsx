import { Suspense, lazy, useCallback, useEffect, useMemo, useState } from "react";
import {
  BarChart3,
  BookOpenText,
  Database,
  FileText,
  GitBranch,
  Globe2,
  Layers3,
  Network,
  Search,
  ShieldCheck
} from "lucide-react";
import type { LucideIcon } from "lucide-react";
import { Chart } from "./components/Chart";
import { InsightsPanel } from "./components/InsightsPanel";
import { StatCard } from "./components/StatCard";
import { TopList } from "./components/TopList";
import { loadArticles, loadCoreData, loadGraph } from "./lib/data";
import type { Article, GraphPayload, SiteData } from "./lib/types";
import { cn, formatNumber } from "./lib/utils";

const ArticlesTable = lazy(() => import("./components/ArticlesTable").then((module) => ({ default: module.ArticlesTable })));
const MapPanel = lazy(() => import("./components/MapPanel").then((module) => ({ default: module.MapPanel })));
const GraphPanel = lazy(() => import("./components/GraphPanel").then((module) => ({ default: module.GraphPanel })));
const ClustersPanel = lazy(() => import("./components/ClustersPanel").then((module) => ({ default: module.ClustersPanel })));
const ProfilesPanel = lazy(() => import("./components/ProfilesPanel").then((module) => ({ default: module.ProfilesPanel })));
const MethodologyPanel = lazy(() => import("./components/MethodologyPanel").then((module) => ({ default: module.MethodologyPanel })));

type View = "overview" | "articles" | "collaborations" | "clusters" | "methodology";

const navItems: Array<{ id: View; label: string; icon: LucideIcon }> = [
  { id: "overview", label: "Synthese", icon: BarChart3 },
  { id: "articles", label: "Articles", icon: Search },
  { id: "collaborations", label: "Collaborations", icon: Network },
  { id: "clusters", label: "Clusters", icon: Layers3 },
  { id: "methodology", label: "Methode", icon: ShieldCheck }
];

export function App() {
  const [data, setData] = useState<SiteData | null>(null);
  const [articles, setArticles] = useState<Article[] | null>(null);
  const [graph, setGraph] = useState<GraphPayload | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [activeView, setActiveView] = useState<View>("overview");

  useEffect(() => {
    loadCoreData()
      .then(setData)
      .catch((reason: unknown) => {
        setError(reason instanceof Error ? reason.message : "Erreur de chargement");
      });
  }, []);

  useEffect(() => {
    if (activeView === "articles" && !articles) {
      loadArticles().then(setArticles).catch((reason: unknown) => {
        setError(reason instanceof Error ? reason.message : "Erreur de chargement des articles");
      });
    }
    if (activeView === "collaborations" && !graph) {
      loadGraph().then(setGraph).catch((reason: unknown) => {
        setError(reason instanceof Error ? reason.message : "Erreur de chargement du graphe");
      });
    }
  }, [activeView, articles, graph]);

  if (error) {
    return (
      <main className="flex min-h-screen items-center justify-center bg-mist p-6">
        <section className="max-w-xl rounded-md border border-line bg-white p-6 shadow-panel">
          <h1 className="text-xl font-semibold text-ink">Chargement impossible</h1>
          <p className="mt-2 text-sm text-slate-600">{error}</p>
        </section>
      </main>
    );
  }

  if (!data) {
    return (
      <main className="flex min-h-screen items-center justify-center bg-mist p-6">
        <LoadingPanel label="Chargement de la synthese" />
      </main>
    );
  }

  return (
    <main className="min-h-screen bg-mist text-ink">
      <header className="sticky top-0 z-30 border-b border-line bg-white/95 backdrop-blur">
        <div className="mx-auto flex max-w-[1500px] flex-col gap-4 px-4 py-4 lg:flex-row lg:items-center lg:justify-between lg:px-6">
          <div className="flex items-center gap-3">
            <div className="flex h-11 w-11 items-center justify-center rounded-md bg-teal text-white">
              <Database className="h-5 w-5" aria-hidden="true" />
            </div>
            <div>
              <h1 className="text-xl font-semibold tracking-tight text-ink">Observatoire IEEE IA</h1>
              <p className="text-sm text-slate-500">
                {formatNumber(data.summary.counts.articles)} articles, {formatNumber(data.summary.counts.authors)} auteurs
              </p>
            </div>
          </div>
          <nav className="flex flex-wrap gap-2">
            {navItems.map((item) => {
              const Icon = item.icon;
              return (
                <button
                  key={item.id}
                  type="button"
                  onClick={() => setActiveView(item.id)}
                  className={cn(
                    "inline-flex h-10 items-center gap-2 rounded-md border px-3 text-sm font-medium transition",
                    activeView === item.id
                      ? "border-teal bg-teal text-white"
                      : "border-line bg-white text-slate-700 hover:border-teal hover:text-teal"
                  )}
                >
                  <Icon className="h-4 w-4" aria-hidden="true" />
                  {item.label}
                </button>
              );
            })}
          </nav>
        </div>
      </header>

      <div className="mx-auto max-w-[1500px] px-4 py-6 lg:px-6">
        <Suspense fallback={<LoadingPanel label="Preparation de la vue" />}>
          {activeView === "overview" ? <Overview data={data} /> : null}
          {activeView === "articles" ? (
            articles ? <ArticlesTable articles={articles} /> : <LoadingPanel label="Chargement du corpus articles" />
          ) : null}
          {activeView === "collaborations" ? (
            <Collaborations data={data} graph={graph} />
          ) : null}
          {activeView === "clusters" ? <Clusters data={data} /> : null}
          {activeView === "methodology" ? <MethodologyPanel data={data} /> : null}
        </Suspense>
      </div>
    </main>
  );
}

function LoadingPanel({ label }: { label: string }) {
  return (
    <section className="w-full rounded-md border border-line bg-white p-5 shadow-panel">
      <div className="flex items-center gap-3">
        <div className="h-3 w-3 animate-pulse rounded-full bg-teal" />
        <p className="text-sm font-medium text-slate-700">{label}</p>
      </div>
      <div className="mt-4 grid gap-3 sm:grid-cols-3">
        <div className="h-20 animate-pulse rounded-md bg-slate-100" />
        <div className="h-20 animate-pulse rounded-md bg-slate-100" />
        <div className="h-20 animate-pulse rounded-md bg-slate-100" />
      </div>
    </section>
  );
}

function Overview({ data }: { data: SiteData }) {
  const timelineOption = useMemo(
    () => ({
      color: ["#0f766e"],
      tooltip: { trigger: "axis" },
      grid: { left: 42, right: 18, top: 20, bottom: 36 },
      xAxis: {
        type: "category",
        data: data.timeline.map((point) => point.year),
        axisTick: { show: false }
      },
      yAxis: { type: "value", splitLine: { lineStyle: { color: "#e8edf4" } } },
      series: [
        {
          type: "line",
          smooth: true,
          symbolSize: 8,
          lineStyle: { width: 3 },
          areaStyle: { color: "rgba(15, 118, 110, 0.12)" },
          data: data.timeline.map((point) => point.count)
        }
      ]
    }),
    [data.timeline]
  );

  const sourceOption = useMemo(
    () => ({
      tooltip: { trigger: "item" },
      legend: { bottom: 0, type: "scroll" },
      series: [
        {
          type: "pie",
          radius: ["46%", "72%"],
          center: ["50%", "44%"],
          itemStyle: { borderColor: "#fff", borderWidth: 2 },
          label: { formatter: "{b}" },
          data: data.sources.map((source) => ({ name: source.label, value: source.count }))
        }
      ]
    }),
    [data.sources]
  );

  const sourceTrendOption = useMemo(() => {
    const years = Array.from(new Set(data.sourceTimeline.map((point) => point.year))).sort();
    const labels = Array.from(new Map(data.sourceTimeline.map((point) => [point.source, point.label])).entries());
    return {
      tooltip: { trigger: "axis" },
      legend: { top: 0, type: "scroll" },
      grid: { left: 42, right: 18, top: 48, bottom: 36 },
      xAxis: { type: "category", data: years, axisTick: { show: false } },
      yAxis: { type: "value", splitLine: { lineStyle: { color: "#e8edf4" } } },
      series: labels.map(([source, label]) => ({
        name: label,
        type: "line",
        smooth: true,
        symbolSize: 5,
        data: years.map((year) => data.sourceTimeline.find((point) => point.source === source && point.year === year)?.count ?? 0)
      }))
    };
  }, [data.sourceTimeline]);

  const keywordOption = useMemo(
    () => ({
      color: ["#2563eb"],
      tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
      grid: { left: 150, right: 20, top: 14, bottom: 22 },
      xAxis: { type: "value", splitLine: { lineStyle: { color: "#e8edf4" } } },
      yAxis: {
        type: "category",
        data: data.keywords.slice(0, 12).map((item) => item.keyword),
        axisTick: { show: false },
        axisLabel: { width: 140, overflow: "truncate" }
      },
      series: [{ type: "bar", data: data.keywords.slice(0, 12).map((item) => item.count), barWidth: 14 }]
    }),
    [data.keywords]
  );

  return (
    <div className="grid gap-5">
      <section className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <StatCard label="Articles" value={data.summary.counts.articles} detail={`${formatNumber(data.summary.coverage.articlesWithKeywords)} avec mots-cles`} icon={FileText} tone="teal" />
        <StatCard label="Auteurs" value={data.summary.counts.authors} detail={`${formatNumber(data.summary.counts.article_authors)} relations article-auteur`} icon={BookOpenText} tone="cobalt" />
        <StatCard label="Laboratoires" value={data.summary.counts.labs} detail={`${formatNumber(data.summary.counts.author_labs)} relations auteur-lab`} icon={Globe2} tone="amber" />
        <StatCard label="Mots-cles" value={data.summary.counts.keywords} detail={`${formatNumber(data.summary.coverage.articlesWithCountry)} articles avec pays`} icon={GitBranch} tone="rosewood" />
      </section>

      <InsightsPanel insights={data.insights} />

      <section className="grid gap-5 xl:grid-cols-[1.5fr_1fr]">
        <ChartPanel title="Publications par annee" option={timelineOption} />
        <ChartPanel title="Sources IEEE" option={sourceOption} />
      </section>

      <section className="grid gap-5 xl:grid-cols-[1fr_1fr]">
        <ChartPanel title="Evolution par source" option={sourceTrendOption} height="h-[26rem]" />
        <ChartPanel title="Mots-cles dominants" option={keywordOption} height="h-[26rem]" />
      </section>

      <section className="grid gap-5 xl:grid-cols-2">
        <TopList
          title="Auteurs productifs"
          items={data.authors.slice(0, 10).map((author) => ({
            label: author.name,
            value: author.articles,
            meta: author.country
          }))}
        />
        <TopList
          title="Pays contributeurs"
          items={data.countries.countries.slice(0, 10).map((country) => ({
            label: country.country,
            value: country.articles,
            meta: `${formatNumber(country.authors)} auteurs`
          }))}
        />
      </section>
    </div>
  );
}

function ChartPanel({ title, option, height = "h-80" }: { title: string; option: Record<string, unknown>; height?: string }) {
  return (
    <section className="rounded-md border border-line bg-white p-4 shadow-panel">
      <h2 className="text-base font-semibold text-ink">{title}</h2>
      <Chart option={option} className={`mt-3 w-full ${height}`} />
    </section>
  );
}

function Collaborations({ data, graph }: { data: SiteData; graph: GraphPayload | null }) {
  const [selectedCountry, setSelectedCountry] = useState<string | null>(null);
  const handleCountrySelect = useCallback((country: string | null) => {
    setSelectedCountry((current) => (country && current === country ? null : country));
  }, []);

  return (
    <div className="grid gap-5">
      <section className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <StatCard label="Pays" value={data.countries.countries.length} detail="pays detectes dans les affiliations" icon={Globe2} tone="teal" />
        <StatCard label="Liens pays" value={data.countries.links.length} detail="collaborations internationales filtrees" icon={GitBranch} tone="cobalt" />
        <StatCard label="Noeuds graphe" value={data.summary.coverage.graphNodes} detail="auteurs les plus connectes" icon={Network} tone="amber" />
        <StatCard label="Aretes graphe" value={data.summary.coverage.graphEdges} detail="co-publications selectionnees" icon={Layers3} tone="rosewood" />
      </section>
      <section className="grid gap-5 xl:grid-cols-[1fr_1fr]">
        <MapPanel data={data.countries} selectedCountry={selectedCountry} onSelectCountry={handleCountrySelect} />
        {graph ? (
          <GraphPanel data={graph} selectedCountry={selectedCountry} onSelectCountry={handleCountrySelect} />
        ) : (
          <LoadingPanel label="Chargement du graphe de co-auteurs" />
        )}
      </section>
      <ProfilesPanel
        authors={data.authors}
        labs={data.labs}
        countries={data.countries.countries}
        selectedCountry={selectedCountry}
        onSelectCountry={handleCountrySelect}
      />
    </div>
  );
}

function Clusters({ data }: { data: SiteData }) {
  const clusterOption = useMemo(
    () => ({
      color: ["#9f384b"],
      tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
      grid: { left: 52, right: 16, top: 18, bottom: 36 },
      xAxis: {
        type: "category",
        data: data.clusters.slice(0, 18).map((cluster) => cluster.cluster),
        axisTick: { show: false }
      },
      yAxis: { type: "value", splitLine: { lineStyle: { color: "#e8edf4" } } },
      series: [{ type: "bar", data: data.clusters.slice(0, 18).map((cluster) => cluster.size), barWidth: 18 }]
    }),
    [data.clusters]
  );

  return (
    <div className="grid gap-5">
      <section className="grid gap-5 xl:grid-cols-[0.9fr_1.1fr]">
        <ChartPanel title="Taille des clusters" option={clusterOption} />
        <TopList
          title="Clusters principaux"
          items={data.clusters.slice(0, 10).map((cluster) => ({
            label: cluster.label,
            value: cluster.size,
            meta: `Cluster ${cluster.cluster} - ${cluster.signalTerms.slice(0, 4).join(", ")}`
          }))}
        />
      </section>
      <ClustersPanel clusters={data.clusters} />
    </div>
  );
}
