import type { Config } from "tailwindcss";

export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      fontFamily: {
        sans: ["Inter", "ui-sans-serif", "system-ui", "sans-serif"]
      },
      colors: {
        ink: "#17202a",
        mist: "#f5f7fb",
        line: "#d9e0ea",
        teal: "#0f766e",
        cobalt: "#2563eb",
        amber: "#b7791f",
        rosewood: "#9f384b"
      },
      boxShadow: {
        panel: "0 1px 2px rgba(15, 23, 42, 0.06), 0 8px 28px rgba(15, 23, 42, 0.07)"
      }
    }
  },
  plugins: []
} satisfies Config;
