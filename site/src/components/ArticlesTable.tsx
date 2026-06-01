import { useMemo, useState } from "react";
import {
  flexRender,
  getCoreRowModel,
  getPaginationRowModel,
  useReactTable,
  type ColumnDef
} from "@tanstack/react-table";
import { ChevronLeft, ChevronRight, ExternalLink, Search, X } from "lucide-react";
import type { Article } from "../lib/types";
import { formatNumber } from "../lib/utils";

type ArticlesTableProps = {
  articles: Article[];
};

export function ArticlesTable({ articles }: ArticlesTableProps) {
  const [query, setQuery] = useState("");
  const [year, setYear] = useState("all");
  const [source, setSource] = useState("all");
  const [country, setCountry] = useState("all");
  const [keyword, setKeyword] = useState("all");
  const [minAuthors, setMinAuthors] = useState("0");
  const [selectedArticle, setSelectedArticle] = useState<Article | null>(null);

  const years = useMemo(
    () => Array.from(new Set(articles.map((item) => item.year).filter(Boolean))).sort((a, b) => Number(b) - Number(a)),
    [articles]
  );
  const sources = useMemo(
    () => Array.from(new Map(articles.map((item) => [item.source, item.sourceLabel])).entries()).sort((a, b) => a[1].localeCompare(b[1])),
    [articles]
  );
  const countries = useMemo(
    () => Array.from(new Set(articles.map((item) => item.country).filter((item) => item !== "Unknown"))).sort(),
    [articles]
  );
  const keywords = useMemo(() => {
    const counts = new Map<string, number>();
    articles.forEach((article) => {
      article.keywords.forEach((item) => counts.set(item, (counts.get(item) ?? 0) + 1));
    });
    return Array.from(counts.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 120)
      .map(([value]) => value);
  }, [articles]);

  const filteredArticles = useMemo(() => {
    const normalized = query.trim().toLowerCase();
    return articles.filter((article) => {
      const matchesQuery =
        !normalized ||
        article.title.toLowerCase().includes(normalized) ||
        article.keywords.join(" ").toLowerCase().includes(normalized) ||
        article.publishedIn.toLowerCase().includes(normalized);
      const matchesYear = year === "all" || String(article.year) === year;
      const matchesSource = source === "all" || article.source === source;
      const matchesCountry = country === "all" || article.country === country;
      const matchesKeyword = keyword === "all" || article.keywords.includes(keyword);
      const matchesAuthors = article.authorCount >= Number(minAuthors);
      return matchesQuery && matchesYear && matchesSource && matchesCountry && matchesKeyword && matchesAuthors;
    });
  }, [articles, country, keyword, minAuthors, query, source, year]);

  const columns = useMemo<ColumnDef<Article>[]>(
    () => [
      {
        accessorKey: "title",
        header: "Article",
        cell: ({ row }) => (
          <div className="max-w-[42rem]">
            <p className="line-clamp-2 font-medium text-ink">{row.original.title}</p>
            <p className="mt-1 line-clamp-1 text-xs text-slate-500">{row.original.publishedIn || row.original.publisher}</p>
          </div>
        )
      },
      {
        accessorKey: "year",
        header: "Annee",
        cell: ({ row }) => <span className="tabular-nums">{row.original.year ?? "-"}</span>
      },
      {
        accessorKey: "sourceLabel",
        header: "Source",
        cell: ({ row }) => <span className="whitespace-nowrap">{row.original.sourceLabel}</span>
      },
      {
        accessorKey: "country",
        header: "Pays",
        cell: ({ row }) => <span className="whitespace-nowrap">{row.original.country}</span>
      },
      {
        accessorKey: "authorCount",
        header: "Auteurs",
        cell: ({ row }) => <span className="tabular-nums">{row.original.authorCount}</span>
      }
    ],
    []
  );

  const table = useReactTable({
    data: filteredArticles,
    columns,
    getCoreRowModel: getCoreRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    initialState: {
      pagination: {
        pageSize: 14
      }
    }
  });

  return (
    <section className="rounded-md border border-line bg-white shadow-panel">
      <div className="border-b border-line p-4">
        <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
          <div>
            <h2 className="text-base font-semibold text-ink">Corpus articles</h2>
            <p className="text-sm text-slate-500">{formatNumber(filteredArticles.length)} articles affiches</p>
          </div>
          <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-[18rem_8rem_13rem_12rem_13rem_8rem]">
            <label className="relative block">
              <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-400" />
              <input
                value={query}
                onChange={(event) => setQuery(event.target.value)}
                className="h-10 w-full rounded-md border border-line bg-white pl-9 pr-3 text-sm outline-none ring-teal/20 transition focus:border-teal focus:ring-4"
                placeholder="Recherche"
              />
            </label>
            <select value={year} onChange={(event) => setYear(event.target.value)} className="h-10 rounded-md border border-line bg-white px-3 text-sm outline-none focus:border-teal">
              <option value="all">Annees</option>
              {years.map((item) => (
                <option key={item} value={String(item)}>{item}</option>
              ))}
            </select>
            <select value={source} onChange={(event) => setSource(event.target.value)} className="h-10 rounded-md border border-line bg-white px-3 text-sm outline-none focus:border-teal">
              <option value="all">Sources</option>
              {sources.map(([value, label]) => (
                <option key={value} value={value}>{label}</option>
              ))}
            </select>
            <select value={country} onChange={(event) => setCountry(event.target.value)} className="h-10 rounded-md border border-line bg-white px-3 text-sm outline-none focus:border-teal">
              <option value="all">Pays</option>
              {countries.slice(0, 120).map((item) => (
                <option key={item} value={item}>{item}</option>
              ))}
            </select>
            <select value={keyword} onChange={(event) => setKeyword(event.target.value)} className="h-10 rounded-md border border-line bg-white px-3 text-sm outline-none focus:border-teal">
              <option value="all">Mots-cles</option>
              {keywords.map((item) => (
                <option key={item} value={item}>{item}</option>
              ))}
            </select>
            <select value={minAuthors} onChange={(event) => setMinAuthors(event.target.value)} className="h-10 rounded-md border border-line bg-white px-3 text-sm outline-none focus:border-teal">
              <option value="0">Auteurs</option>
              <option value="2">2+</option>
              <option value="5">5+</option>
              <option value="10">10+</option>
            </select>
          </div>
        </div>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full min-w-[820px] text-left text-sm">
          <thead className="bg-slate-50 text-xs uppercase tracking-wide text-slate-500">
            {table.getHeaderGroups().map((headerGroup) => (
              <tr key={headerGroup.id}>
                {headerGroup.headers.map((header) => (
                  <th key={header.id} className="px-4 py-3 font-semibold">
                    {flexRender(header.column.columnDef.header, header.getContext())}
                  </th>
                ))}
              </tr>
            ))}
          </thead>
          <tbody className="divide-y divide-line">
            {table.getRowModel().rows.map((row) => (
              <tr
                key={row.id}
                className="cursor-pointer align-top transition hover:bg-slate-50"
                onClick={() => setSelectedArticle(row.original)}
              >
                {row.getVisibleCells().map((cell) => (
                  <td key={cell.id} className="px-4 py-3 text-slate-700">
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="flex items-center justify-between border-t border-line px-4 py-3 text-sm text-slate-600">
        <span>
          Page {table.getState().pagination.pageIndex + 1} / {table.getPageCount() || 1}
        </span>
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={() => table.previousPage()}
            disabled={!table.getCanPreviousPage()}
            className="inline-flex h-9 w-9 items-center justify-center rounded-md border border-line bg-white text-slate-600 transition hover:border-teal hover:text-teal disabled:cursor-not-allowed disabled:opacity-40"
            aria-label="Page precedente"
          >
            <ChevronLeft className="h-4 w-4" />
          </button>
          <button
            type="button"
            onClick={() => table.nextPage()}
            disabled={!table.getCanNextPage()}
            className="inline-flex h-9 w-9 items-center justify-center rounded-md border border-line bg-white text-slate-600 transition hover:border-teal hover:text-teal disabled:cursor-not-allowed disabled:opacity-40"
            aria-label="Page suivante"
          >
            <ChevronRight className="h-4 w-4" />
          </button>
        </div>
      </div>
      {selectedArticle ? (
        <ArticleDetails article={selectedArticle} onClose={() => setSelectedArticle(null)} />
      ) : null}
    </section>
  );
}

function ArticleDetails({ article, onClose }: { article: Article; onClose: () => void }) {
  const doiUrl = article.doi ? `https://doi.org/${article.doi}` : "";

  return (
    <div className="fixed inset-0 z-50 flex justify-end bg-slate-950/30" role="dialog" aria-modal="true">
      <button type="button" className="absolute inset-0 cursor-default" aria-label="Fermer" onClick={onClose} />
      <aside className="relative h-full w-full max-w-2xl overflow-y-auto bg-white p-6 shadow-2xl">
        <div className="flex items-start justify-between gap-4">
          <div>
            <p className="text-xs font-medium uppercase tracking-wide text-teal">{article.sourceLabel}</p>
            <h3 className="mt-2 text-xl font-semibold leading-snug text-ink">{article.title}</h3>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="inline-flex h-9 w-9 shrink-0 items-center justify-center rounded-md border border-line text-slate-600 transition hover:border-teal hover:text-teal"
            aria-label="Fermer"
          >
            <X className="h-4 w-4" />
          </button>
        </div>

        <dl className="mt-5 grid gap-3 rounded-md border border-line bg-slate-50 p-4 sm:grid-cols-2">
          <div>
            <dt className="text-xs uppercase tracking-wide text-slate-500">Annee</dt>
            <dd className="mt-1 text-sm font-medium text-ink">{article.year ?? "-"}</dd>
          </div>
          <div>
            <dt className="text-xs uppercase tracking-wide text-slate-500">Pays dominant</dt>
            <dd className="mt-1 text-sm font-medium text-ink">{article.country}</dd>
          </div>
          <div>
            <dt className="text-xs uppercase tracking-wide text-slate-500">Auteurs</dt>
            <dd className="mt-1 text-sm font-medium text-ink">{formatNumber(article.authorCount)}</dd>
          </div>
          <div>
            <dt className="text-xs uppercase tracking-wide text-slate-500">Publication</dt>
            <dd className="mt-1 line-clamp-2 text-sm font-medium text-ink">{article.publishedIn || article.publisher || "-"}</dd>
          </div>
        </dl>

        <section className="mt-5">
          <h4 className="text-sm font-semibold text-ink">Resume</h4>
          <p className="mt-2 text-sm leading-6 text-slate-700">{article.abstract || "Resume indisponible."}</p>
        </section>

        <section className="mt-5">
          <h4 className="text-sm font-semibold text-ink">Mots-cles</h4>
          <div className="mt-2 flex flex-wrap gap-2">
            {article.keywords.length ? article.keywords.map((item) => (
              <span key={item} className="rounded-sm bg-slate-100 px-2 py-1 text-xs text-slate-700 ring-1 ring-line">
                {item}
              </span>
            )) : <span className="text-sm text-slate-500">Aucun mot-cle exporte.</span>}
          </div>
        </section>

        {doiUrl ? (
          <a
            href={doiUrl}
            target="_blank"
            rel="noreferrer"
            className="mt-6 inline-flex h-10 items-center gap-2 rounded-md bg-teal px-4 text-sm font-medium text-white transition hover:bg-teal/90"
          >
            <ExternalLink className="h-4 w-4" aria-hidden="true" />
            Ouvrir le DOI
          </a>
        ) : null}
      </aside>
    </div>
  );
}
