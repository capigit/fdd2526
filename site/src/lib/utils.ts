import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function formatNumber(value: number | undefined | null) {
  return new Intl.NumberFormat("fr-FR").format(value ?? 0);
}

export function colorFromString(value: string) {
  let hash = 0;
  for (let index = 0; index < value.length; index += 1) {
    hash = value.charCodeAt(index) + ((hash << 5) - hash);
  }
  const palette = ["#0f766e", "#2563eb", "#b7791f", "#9f384b", "#6d5bd0", "#2f855a", "#c05621"];
  return palette[Math.abs(hash) % palette.length];
}

export function clamp(value: number, min: number, max: number) {
  return Math.max(min, Math.min(max, value));
}
