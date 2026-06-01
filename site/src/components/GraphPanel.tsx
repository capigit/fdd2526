import { useEffect, useMemo, useRef, useState } from "react";
import Graph from "graphology";
import Sigma from "sigma";
import { Search } from "lucide-react";
import type { GraphPayload } from "../lib/types";
import { clamp, colorFromString, formatNumber } from "../lib/utils";

type GraphPanelProps = {
  data: GraphPayload;
  selectedCountry: string | null;
  onSelectCountry: (country: string | null) => void;
};

export function GraphPanel({ data, selectedCountry, onSelectCountry }: GraphPanelProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const sigmaRef = useRef<Sigma | null>(null);
  const graphRef = useRef<Graph | null>(null);
  const [query, setQuery] = useState("");

  const matchedNode = useMemo(() => {
    const normalized = query.trim().toLowerCase();
    if (!normalized) {
      return null;
    }
    return data.nodes.find((node) => node.label.toLowerCase().includes(normalized)) ?? null;
  }, [data.nodes, query]);

  useEffect(() => {
    if (!containerRef.current) {
      return;
    }

    const graph = new Graph();
    const count = Math.max(data.nodes.length, 1);

    data.nodes.forEach((node, index) => {
      const angle = (index / count) * Math.PI * 2;
      const radius = 8 + Math.log(node.degree + 1) * 2;
      graph.addNode(node.id, {
        x: Math.cos(angle) * radius,
        y: Math.sin(angle) * radius,
        label: node.label,
        size: clamp(3 + Math.sqrt(node.degree), 4, 18),
        baseColor: colorFromString(node.country),
        color: colorFromString(node.country),
        country: node.country,
        articles: node.articles,
        degree: node.degree
      });
    });

    data.edges.forEach((edge) => {
      if (graph.hasNode(edge.source) && graph.hasNode(edge.target)) {
        graph.addEdgeWithKey(edge.id, edge.source, edge.target, {
          size: clamp(edge.weight, 1, 6),
          color: "rgba(99, 116, 139, 0.28)"
        });
      }
    });

    const sigma = new Sigma(graph, containerRef.current, {
      allowInvalidContainer: true,
      renderEdgeLabels: false,
      defaultEdgeColor: "rgba(99, 116, 139, 0.28)",
      labelDensity: 0.06,
      labelRenderedSizeThreshold: 11,
      zIndex: true
    } as any);

    graphRef.current = graph;
    sigmaRef.current = sigma;

    sigma.on("clickNode", ({ node }) => {
      const country = graph.getNodeAttribute(node, "country") as string;
      onSelectCountry(country);
    });

    return () => {
      sigma.kill();
      graph.clear();
      graphRef.current = null;
      sigmaRef.current = null;
    };
  }, [data, onSelectCountry]);

  useEffect(() => {
    const sigma = sigmaRef.current;
    const graph = graphRef.current;
    if (!sigma || !graph) {
      return;
    }

    graph.forEachNode((node, attributes) => {
      const country = attributes.country as string;
      const baseColor = attributes.baseColor as string;
      const isSelected = !selectedCountry || country === selectedCountry;
      graph.setNodeAttribute(node, "color", isSelected ? baseColor : "rgba(148, 163, 184, 0.28)");
      graph.setNodeAttribute(node, "zIndex", isSelected ? 2 : 0);
    });
    sigma.refresh();
  }, [selectedCountry]);

  useEffect(() => {
    const sigma = sigmaRef.current;
    const graph = graphRef.current;
    if (!sigma || !graph || !matchedNode || !graph.hasNode(matchedNode.id)) {
      return;
    }

    const attrs = graph.getNodeAttributes(matchedNode.id) as { x: number; y: number };
    sigma.getCamera().animate({ x: attrs.x, y: attrs.y, ratio: 0.12 }, { duration: 500 });
  }, [matchedNode]);

  return (
    <section className="rounded-md border border-line bg-white shadow-panel">
      <div className="flex flex-col gap-3 border-b border-line p-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h2 className="text-base font-semibold text-ink">Graphe de co-auteurs</h2>
          <p className="text-sm text-slate-500">
            {formatNumber(data.nodes.length)} auteurs, {formatNumber(data.edges.length)} liens
            {selectedCountry ? ` - filtre pays : ${selectedCountry}` : ""}
          </p>
        </div>
        <label className="relative block sm:w-72">
          <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-400" />
          <input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            className="h-10 w-full rounded-md border border-line bg-white pl-9 pr-3 text-sm outline-none ring-teal/20 transition focus:border-teal focus:ring-4"
            placeholder="Auteur"
          />
        </label>
      </div>
      <div ref={containerRef} className="h-[34rem] w-full rounded-b-md bg-slate-50" />
    </section>
  );
}
