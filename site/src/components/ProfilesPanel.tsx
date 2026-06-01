import { useMemo, useState } from "react";
import { Building2, Globe2, UserRound } from "lucide-react";
import type { AuthorPoint, CountryPoint, LabPoint } from "../lib/types";
import { cn, formatNumber } from "../lib/utils";

type ProfilesPanelProps = {
  authors: AuthorPoint[];
  labs: LabPoint[];
  countries: CountryPoint[];
  selectedCountry: string | null;
  onSelectCountry: (country: string | null) => void;
};

type Tab = "authors" | "labs" | "countries";

export function ProfilesPanel({ authors, labs, countries, selectedCountry, onSelectCountry }: ProfilesPanelProps) {
  const [tab, setTab] = useState<Tab>("authors");
  const [query, setQuery] = useState("");

  const rows = useMemo(() => {
    const normalized = query.trim().toLowerCase();
    if (tab === "authors") {
      return authors
        .filter((item) => !selectedCountry || item.country === selectedCountry)
        .filter((item) => !normalized || `${item.name} ${item.country}`.toLowerCase().includes(normalized))
        .slice(0, 24)
        .map((item) => ({
          id: `author-${item.id}`,
          title: item.name,
          meta: item.country,
          country: item.country,
          valueLabel: "articles",
          value: item.articles,
          icon: UserRound
        }));
    }
    if (tab === "labs") {
      return labs
        .filter((item) => !selectedCountry || item.country === selectedCountry)
        .filter((item) => !normalized || `${item.name} ${item.country}`.toLowerCase().includes(normalized))
        .slice(0, 24)
        .map((item) => ({
          id: `lab-${item.id}`,
          title: item.name,
          meta: item.country,
          country: item.country,
          valueLabel: "auteurs",
          value: item.authors,
          icon: Building2
        }));
    }
    return countries
      .filter((item) => !selectedCountry || item.country === selectedCountry)
      .filter((item) => !normalized || item.country.toLowerCase().includes(normalized))
      .slice(0, 24)
      .map((item) => ({
        id: `country-${item.country}`,
        title: item.country,
        meta: `${formatNumber(item.authors)} auteurs, ${formatNumber(item.labs)} labs`,
        country: item.country,
        valueLabel: "articles",
        value: item.articles,
        icon: Globe2
      }));
  }, [authors, countries, labs, query, selectedCountry, tab]);

  return (
    <section className="rounded-md border border-line bg-white shadow-panel">
      <div className="flex flex-col gap-3 border-b border-line p-4 lg:flex-row lg:items-center lg:justify-between">
        <div>
          <h2 className="text-base font-semibold text-ink">Profils du corpus</h2>
          <p className="text-sm text-slate-500">
            {selectedCountry ? `Focus pays : ${selectedCountry}` : "Auteurs, laboratoires et pays les plus structurants"}
          </p>
        </div>
        <div className="flex flex-col gap-2 sm:flex-row sm:items-center">
          <div className="inline-flex rounded-md border border-line bg-slate-50 p-1">
            {[
              ["authors", "Auteurs"],
              ["labs", "Labs"],
              ["countries", "Pays"]
            ].map(([value, label]) => (
              <button
                key={value}
                type="button"
                onClick={() => setTab(value as Tab)}
                className={cn(
                  "h-8 rounded-sm px-3 text-sm font-medium transition",
                  tab === value ? "bg-white text-teal shadow-sm" : "text-slate-600 hover:text-ink"
                )}
              >
                {label}
              </button>
            ))}
          </div>
          <input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            className="h-10 rounded-md border border-line bg-white px-3 text-sm outline-none ring-teal/20 transition focus:border-teal focus:ring-4"
            placeholder="Filtrer"
          />
          {selectedCountry ? (
            <button
              type="button"
              onClick={() => onSelectCountry(null)}
              className="h-10 rounded-md border border-line px-3 text-sm font-medium text-slate-600 transition hover:border-teal hover:text-teal"
            >
              Effacer
            </button>
          ) : null}
        </div>
      </div>
      <div className="grid gap-3 p-4 sm:grid-cols-2 xl:grid-cols-3">
        {rows.map((row) => {
          const Icon = row.icon;
          return (
            <article
              key={row.id}
              className="flex cursor-pointer gap-3 rounded-md border border-line bg-slate-50 p-3 transition hover:border-teal"
              onClick={() => onSelectCountry(row.country)}
            >
              <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-md bg-white text-teal ring-1 ring-line">
                <Icon className="h-5 w-5" aria-hidden="true" />
              </div>
              <div className="min-w-0 flex-1">
                <p className="truncate text-sm font-semibold text-ink">{row.title}</p>
                <p className="mt-1 truncate text-xs text-slate-500">{row.meta}</p>
                <p className="mt-2 text-sm text-slate-700">
                  <span className="font-semibold">{formatNumber(row.value)}</span> {row.valueLabel}
                </p>
              </div>
            </article>
          );
        })}
      </div>
    </section>
  );
}
