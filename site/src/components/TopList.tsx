import { formatNumber } from "../lib/utils";

type TopListProps = {
  title: string;
  items: Array<{ label: string; value: number; meta?: string }>;
};

export function TopList({ title, items }: TopListProps) {
  const max = Math.max(...items.map((item) => item.value), 1);

  return (
    <section className="rounded-md border border-line bg-white p-4 shadow-panel">
      <h2 className="text-base font-semibold text-ink">{title}</h2>
      <div className="mt-4 space-y-3">
        {items.map((item) => (
          <div key={`${item.label}-${item.meta ?? ""}`} className="grid gap-1">
            <div className="flex items-baseline justify-between gap-3 text-sm">
              <span className="truncate font-medium text-slate-800">{item.label}</span>
              <span className="shrink-0 tabular-nums text-slate-500">{formatNumber(item.value)}</span>
            </div>
            <div className="h-2 rounded-sm bg-slate-100">
              <div
                className="h-2 rounded-sm bg-teal"
                style={{ width: `${Math.max(4, (item.value / max) * 100)}%` }}
              />
            </div>
            {item.meta ? <p className="truncate text-xs text-slate-500">{item.meta}</p> : null}
          </div>
        ))}
      </div>
    </section>
  );
}
