import { CheckCircle2, Database, FileJson, GitBranch, ShieldAlert } from "lucide-react";
import type { LucideIcon } from "lucide-react";
import type { SiteData } from "../lib/types";
import { formatNumber } from "../lib/utils";

type MethodologyPanelProps = {
  data: SiteData;
};

export function MethodologyPanel({ data }: MethodologyPanelProps) {
  const steps: Array<{ title: string; text: string; icon: LucideIcon }> = [
    { title: "Import JSON", text: "Transformation des sources IEEE par theme vers SQLite.", icon: Database },
    { title: "Fusion", text: "Remapping des IDs articles, auteurs et laboratoires.", icon: GitBranch },
    { title: "Analyse", text: "Extraction des indicateurs, graphes, clusters et profils.", icon: CheckCircle2 },
    { title: "Publication", text: "Export JSON statique compatible Netlify/GitHub Pages.", icon: FileJson }
  ];

  return (
    <div className="grid gap-5">
      <section className="rounded-md border border-line bg-white p-5 shadow-panel">
        <div className="flex items-start gap-3">
          <div className="flex h-10 w-10 items-center justify-center rounded-md bg-teal/10 text-teal">
            <GitBranch className="h-5 w-5" aria-hidden="true" />
          </div>
          <div>
            <h2 className="text-lg font-semibold text-ink">Methodologie de traitement</h2>
            <p className="mt-1 max-w-3xl text-sm leading-6 text-slate-600">
              Les donnees sont importees depuis des exports JSON IEEE, harmonisees en bases SQLite, puis fusionnees dans une base relationnelle unique. Le site ne lit pas SQLite directement: il consomme des JSON statiques produits au build.
            </p>
          </div>
        </div>
      </section>

      <section className="grid gap-4 lg:grid-cols-4">
        {steps.map(({ title, text, icon: Icon }) => (
          <article key={title} className="rounded-md border border-line bg-white p-4 shadow-panel">
            <Icon className="h-5 w-5 text-teal" aria-hidden="true" />
            <h3 className="mt-3 text-sm font-semibold text-ink">{title}</h3>
            <p className="mt-2 text-sm leading-6 text-slate-600">{text}</p>
          </article>
        ))}
      </section>

      <section className="grid gap-5 xl:grid-cols-[1fr_1fr]">
        <article className="rounded-md border border-line bg-white p-5 shadow-panel">
          <h3 className="text-base font-semibold text-ink">Qualite et couverture</h3>
          <dl className="mt-4 grid gap-3 sm:grid-cols-2">
            <Metric label="Articles" value={data.summary.counts.articles} />
            <Metric label="Auteurs" value={data.summary.counts.authors} />
            <Metric label="Articles avec mots-cles" value={data.summary.coverage.articlesWithKeywords} />
            <Metric label="Articles avec pays" value={data.summary.coverage.articlesWithCountry} />
            <Metric label="Noeuds graphe publies" value={data.summary.coverage.graphNodes} />
            <Metric label="Aretes graphe publiees" value={data.summary.coverage.graphEdges} />
          </dl>
        </article>

        <article className="rounded-md border border-line bg-white p-5 shadow-panel">
          <div className="flex items-center gap-2">
            <ShieldAlert className="h-5 w-5 text-amber" aria-hidden="true" />
            <h3 className="text-base font-semibold text-ink">Limites d'interpretation</h3>
          </div>
          <ul className="mt-4 space-y-3 text-sm leading-6 text-slate-600">
            <li>Les pays sont deduits des affiliations; certaines affiliations restent ambigues ou contiennent des emails/abreviations.</li>
            <li>Les clusters sont des regroupements exploratoires, pas des categories scientifiques verifiees manuellement.</li>
            <li>Le graphe public est volontairement limite aux auteurs les plus connectes pour garder une navigation fluide.</li>
            <li>Les tendances dependent du corpus IEEE collecte, pas de l'ensemble de la litterature scientifique mondiale.</li>
          </ul>
        </article>
      </section>

      <section className="rounded-md border border-line bg-white p-5 shadow-panel">
        <h3 className="text-base font-semibold text-ink">Checklist de deploiement Netlify</h3>
        <div className="mt-4 grid gap-3 md:grid-cols-2">
          {[
            "Regenerer les JSON: python scripts_exports/export_site_data.py",
            "Verifier le build: npm run build",
            "Base directory Netlify: site",
            "Build command: npm run build",
            "Publish directory: site/dist",
            "Ne pas publier bd/*.db, analyse/*.pkl, node_modules ou dist dans Git"
          ].map((item) => (
            <div key={item} className="flex items-start gap-2 rounded-md bg-slate-50 p-3 text-sm text-slate-700">
              <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0 text-teal" aria-hidden="true" />
              <span>{item}</span>
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}

function Metric({ label, value }: { label: string; value: number }) {
  return (
    <div className="rounded-md border border-line bg-slate-50 p-3">
      <dt className="text-xs uppercase tracking-wide text-slate-500">{label}</dt>
      <dd className="mt-1 text-lg font-semibold text-ink">{formatNumber(value)}</dd>
    </div>
  );
}
