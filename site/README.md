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

Avec Node.js disponible :

```powershell
npm install
npm run dev
```

Avec l'environnement Anaconda utilise sur ce poste :

```powershell
conda run -n claude-dev npm install
conda run -n claude-dev npm run dev -- --port 5173
```

## Donnees

Les donnees du site sont lues depuis `public/data/*.json`.

Depuis la racine du projet :

```powershell
python .\scripts_exports\export_site_data.py
```

## Build

```powershell
npm run build
```

Ou via conda :

```powershell
conda run -n claude-dev npm run build
```

## Netlify

- Base directory : `site`
- Build command : `npm run build`
- Publish directory : `dist`

Le fichier `netlify.toml` contient deja ces parametres. Un `netlify.toml` existe aussi a la racine du projet pour faciliter la connexion du depot complet depuis Netlify.
