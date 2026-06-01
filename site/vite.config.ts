import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  base: "./",
  plugins: [react()],
  build: {
    sourcemap: false,
    chunkSizeWarningLimit: 1200,
    rollupOptions: {
      output: {
        manualChunks: {
          charts: ["echarts"],
          maps: ["maplibre-gl"],
          graph: ["sigma", "graphology"],
          react: ["react", "react-dom"]
        }
      }
    }
  }
});
