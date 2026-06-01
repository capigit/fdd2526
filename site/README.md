# Observatoire IEEE IA

Application statique de restitution pour le projet de fouille de donnees IEEE.

## Stack

- Vite
- React
- TypeScript
- Tailwind CSS
- Apache ECharts
- MapLibre GL JS
- Sigma.js
- TanStack Table

## Developpement local

```bash
npm install
npm run dev
```

## Donnees

Les donnees du site sont lues depuis `public/data/*.json`.

Depuis le dossier `site/` :

```bash
cd ..
python scripts_exports/export_site_data.py
```

## Build

```bash
npm run build
```

## Netlify

- Base directory : `site`
- Build command : `npm run build`
- Publish directory : `dist`

Le fichier `netlify.toml` contient deja ces parametres. Un `netlify.toml` existe aussi a la racine du projet pour faciliter la connexion du depot complet depuis Netlify.
