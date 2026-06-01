import type { LucideIcon } from "lucide-react";
import { formatNumber } from "../lib/utils";

type StatCardProps = {
  label: string;
  value: number;
  detail: string;
  icon: LucideIcon;
  tone: "teal" | "cobalt" | "amber" | "rosewood";
};

const toneClasses = {
  teal: "bg-teal/10 text-teal",
  cobalt: "bg-cobalt/10 text-cobalt",
  amber: "bg-amber/10 text-amber",
  rosewood: "bg-rosewood/10 text-rosewood"
};

export function StatCard({ label, value, detail, icon: Icon, tone }: StatCardProps) {
  return (
    <section className="rounded-md border border-line bg-white p-4 shadow-panel">
      <div className="flex items-start justify-between gap-3">
        <div>
          <p className="text-xs font-medium uppercase tracking-wide text-slate-500">{label}</p>
          <p className="mt-2 text-2xl font-semibold text-ink">{formatNumber(value)}</p>
        </div>
        <div className={`flex h-10 w-10 items-center justify-center rounded-md ${toneClasses[tone]}`}>
          <Icon className="h-5 w-5" aria-hidden="true" />
        </div>
      </div>
      <p className="mt-3 text-sm text-slate-600">{detail}</p>
    </section>
  );
}
