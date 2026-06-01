import { ArrowUpRight, Sparkles } from "lucide-react";
import type { Insight } from "../lib/types";
import { cn, formatNumber } from "../lib/utils";

type InsightsPanelProps = {
  insights: Insight[];
};

const toneClasses = {
  teal: "border-teal/30 bg-teal/5 text-teal",
  cobalt: "border-cobalt/30 bg-cobalt/5 text-cobalt",
  amber: "border-amber/30 bg-amber/5 text-amber",
  rosewood: "border-rosewood/30 bg-rosewood/5 text-rosewood"
};

export function InsightsPanel({ insights }: InsightsPanelProps) {
  return (
    <section className="rounded-md border border-line bg-white shadow-panel">
      <div className="flex flex-col gap-3 border-b border-line p-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <div className="flex items-center gap-2">
            <Sparkles className="h-5 w-5 text-teal" aria-hidden="true" />
            <h2 className="text-base font-semibold text-ink">Insights cles</h2>
          </div>
          <p className="mt-1 text-sm text-slate-500">Lecture automatique des signaux les plus visibles du corpus</p>
        </div>
        <span className="rounded-sm bg-slate-50 px-3 py-1 text-xs font-medium text-slate-600 ring-1 ring-line">
          {formatNumber(insights.length)} constats
        </span>
      </div>
      <div className="grid gap-3 p-4 md:grid-cols-2 xl:grid-cols-3">
        {insights.map((insight) => (
          <article key={`${insight.title}-${insight.value}`} className={cn("rounded-md border p-4", toneClasses[insight.tone])}>
            <div className="flex items-start justify-between gap-3">
              <div>
                <p className="text-xs font-semibold uppercase tracking-wide opacity-80">{insight.title}</p>
                <h3 className="mt-2 line-clamp-2 text-lg font-semibold text-ink">{insight.value}</h3>
              </div>
              <ArrowUpRight className="h-4 w-4 shrink-0" aria-hidden="true" />
            </div>
            <p className="mt-3 text-sm leading-6 text-slate-700">{insight.detail}</p>
          </article>
        ))}
      </div>
    </section>
  );
}
