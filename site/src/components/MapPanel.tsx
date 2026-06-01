import { useEffect, useRef } from "react";
import maplibregl from "maplibre-gl";
import type { CountriesPayload } from "../lib/types";
import { clamp, colorFromString, formatNumber } from "../lib/utils";

type MapPanelProps = {
  data: CountriesPayload;
  selectedCountry: string | null;
  onSelectCountry: (country: string | null) => void;
};

export function MapPanel({ data, selectedCountry, onSelectCountry }: MapPanelProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!containerRef.current) {
      return;
    }

    const map = new maplibregl.Map({
      container: containerRef.current,
      style: "https://demotiles.maplibre.org/style.json",
      center: [12, 28],
      zoom: 1.25,
      attributionControl: { compact: true }
    });

    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-right");

    const markers: maplibregl.Marker[] = [];

    map.on("load", () => {
      const features = data.links
        .map((link) => {
          const source = data.countries.find((country) => country.country === link.source);
          const target = data.countries.find((country) => country.country === link.target);
          if (!source?.coordinates || !target?.coordinates) {
            return null;
          }
          return {
            type: "Feature",
            properties: { count: link.count },
            geometry: {
              type: "LineString",
              coordinates: [
                [source.coordinates[1], source.coordinates[0]],
                [target.coordinates[1], target.coordinates[0]]
              ]
            }
          };
        })
        .filter(Boolean);

      map.addSource("collaboration-links", {
        type: "geojson",
        data: {
          type: "FeatureCollection",
          features
        }
      } as maplibregl.GeoJSONSourceSpecification);

      map.addLayer({
        id: "collaboration-links",
        type: "line",
        source: "collaboration-links",
        paint: {
          "line-color": "#0f766e",
          "line-opacity": 0.42,
          "line-width": ["interpolate", ["linear"], ["get", "count"], 2, 1, 20, 5, 80, 10]
        }
      });

      data.countries
        .filter((country) => country.coordinates)
        .slice(0, 80)
        .forEach((country) => {
          const size = clamp(10 + Math.sqrt(country.articles), 16, 42);
          const element = document.createElement("button");
          element.type = "button";
          element.className = "map-marker";
          element.style.width = `${size}px`;
          element.style.height = `${size}px`;
          element.style.backgroundColor = colorFromString(country.country);
          element.style.opacity = selectedCountry && selectedCountry !== country.country ? "0.34" : "0.92";
          element.style.outline = selectedCountry === country.country ? "3px solid #f6c15b" : "none";
          element.title = `${country.country} - ${formatNumber(country.articles)} articles`;
          element.addEventListener("click", () => onSelectCountry(country.country));

          const popup = new maplibregl.Popup({ offset: 18 }).setHTML(
            `<strong>${country.country}</strong><br>${formatNumber(country.articles)} articles<br>${formatNumber(country.authors)} auteurs<br>${formatNumber(country.labs)} labs`
          );

          const marker = new maplibregl.Marker({ element })
            .setLngLat([country.coordinates![1], country.coordinates![0]])
            .setPopup(popup)
            .addTo(map);
          markers.push(marker);
        });
    });

    return () => {
      markers.forEach((marker) => marker.remove());
      map.remove();
    };
  }, [data, onSelectCountry, selectedCountry]);

  return (
    <section className="rounded-md border border-line bg-white shadow-panel">
      <div className="border-b border-line p-4">
        <div className="flex items-start justify-between gap-3">
          <div>
            <h2 className="text-base font-semibold text-ink">Collaborations internationales</h2>
            <p className="text-sm text-slate-500">{formatNumber(data.links.length)} liens significatifs</p>
          </div>
          {selectedCountry ? (
            <button
              type="button"
              onClick={() => onSelectCountry(null)}
              className="h-8 rounded-md border border-line px-3 text-xs font-medium text-slate-600 transition hover:border-teal hover:text-teal"
            >
              Effacer
            </button>
          ) : null}
        </div>
      </div>
      <div ref={containerRef} className="h-[34rem] w-full overflow-hidden rounded-b-md" />
    </section>
  );
}
