import { useMemo, useState } from "react";
import { Search } from "lucide-react";
import type { ClusterSummary } from "../lib/types";
import { cn, formatNumber } from "../lib/utils";

type ClustersPanelProps = {
  clusters: ClusterSummary[];
};

export function ClustersPanel({ clusters }: ClustersPanelProps) {
  const [query, setQuery] = useState("");
  const [selectedCluster, setSelectedCluster] = useState(clusters[0]?.cluster ?? "");

  const filteredClusters = useMemo(() => {
    const normalized = query.trim().toLowerCase();
    return clusters.filter((cluster) => {
      if (!normalized) {
        return true;
      }
      return (
        cluster.cluster.includes(normalized) ||
        cluster.label.toLowerCase().includes(normalized) ||
        cluster.topCountry.toLowerCase().includes(normalized) ||
        cluster.topTerms.some((term) => term.term.includes(normalized)) ||
        cluster.signalTerms.some((term) => term.includes(normalized))
      );
    });
  }, [clusters, query]);

  const selected = clusters.find((cluster) => cluster.cluster === selectedCluster) ?? filteredClusters[0] ?? clusters[0];

  return (
    <section className="rounded-md border border-line bg-white shadow-panel">
      <div className="flex flex-col gap-3 border-b border-line p-4 lg:flex-row lg:items-center lg:justify-between">
        <div>
          <h2 className="text-base font-semibold text-ink">Exploration des clusters</h2>
          <p className="text-sm text-slate-500">{formatNumber(clusters.length)} groupes thematiques detectes</p>
        </div>
        <label className="relative block lg:w-80">
          <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-400" />
          <input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            className="h-10 w-full rounded-md border border-line bg-white pl-9 pr-3 text-sm outline-none ring-teal/20 transition focus:border-teal focus:ring-4"
            placeholder="Cluster, pays ou terme"
          />
        </label>
      </div>
      <div className="grid min-h-[32rem] lg:grid-cols-[22rem_1fr]">
        <div className="max-h-[42rem] overflow-y-auto border-b border-line p-3 lg:border-b-0 lg:border-r">
          <div className="space-y-2">
            {filteredClusters.map((cluster) => (
              <button
                key={cluster.cluster}
                type="button"
                onClick={() => setSelectedCluster(cluster.cluster)}
                className={cn(
                  "w-full rounded-md border p-3 text-left transition",
                  selected?.cluster === cluster.cluster
                    ? "border-teal bg-teal/5"
                    : "border-line bg-slate-50 hover:border-teal"
                )}
              >
                <div className="flex items-center justify-between gap-3">
                  <span className="text-sm font-semibold text-ink">{cluster.label}</span>
                  <span className="rounded-sm bg-white px-2 py-1 text-xs text-slate-600 ring-1 ring-line">
                    {formatNumber(cluster.size)}
                  </span>
                </div>
                <p className="mt-2 truncate text-xs text-slate-500">
                  Cluster {cluster.cluster} - {cluster.signalTerms.slice(0, 4).join(", ")}
                </p>
              </button>
            ))}
          </div>
        </div>

        {selected ? (
          <article className="p-5">
            <div className="flex flex-col gap-4 border-b border-line pb-5 sm:flex-row sm:items-start sm:justify-between">
              <div>
                <p className="text-xs font-medium uppercase tracking-wide text-teal">Cluster {selected.cluster}</p>
                <h3 className="mt-1 text-2xl font-semibold text-ink">{selected.label}</h3>
                <p className="mt-2 text-sm text-slate-600">
                  {selected.description}
                </p>
              </div>
              <div className="rounded-md border border-line bg-slate-50 px-4 py-3">
                <p className="text-xs uppercase tracking-wide text-slate-500">Moyenne auteurs</p>
                <p className="mt-1 text-xl font-semibold text-ink">{selected.avgAuthors}</p>
              </div>
            </div>

            <section className="mt-5">
              <h4 className="text-sm font-semibold text-ink">Termes caracteristiques</h4>
              <div className="mt-2 flex flex-wrap gap-2">
                {selected.signalTerms.map((term) => (
                  <span key={term} className="rounded-sm bg-teal/10 px-2 py-1 text-xs font-medium text-teal">
                    {term}
                  </span>
                ))}
              </div>
              <div className="mt-4 grid gap-3 sm:grid-cols-2">
                {selected.topTerms.map((term) => (
                  <div key={term.term} className="rounded-md border border-line bg-slate-50 p-3">
                    <div className="flex items-center justify-between gap-3 text-sm">
                      <span className="font-medium text-ink">{term.term}</span>
                      <span className="tabular-nums text-slate-500">{formatNumber(term.count)}</span>
                    </div>
                    <div className="mt-2 h-2 rounded-sm bg-white">
                      <div
                        className="h-2 rounded-sm bg-rosewood"
                        style={{ width: `${Math.max(8, (term.count / selected.topTerms[0].count) * 100)}%` }}
                      />
                    </div>
                  </div>
                ))}
              </div>
            </section>
          </article>
        ) : null}
      </div>
    </section>
  );
}
