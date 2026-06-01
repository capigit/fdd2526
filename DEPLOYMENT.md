# Publication du projet

Ce projet se publie comme une application statique Netlify. Le pipeline Python reste local et produit les fichiers JSON lus par l'interface React.

## Principe

- Les scripts Python generent les donnees dans `site/public/data/`.
- L'application Vite construit le site final dans `site/dist/`.
- Netlify deploie uniquement le contenu genere par le build frontend.
- Les bases SQLite, modeles `.pkl`, environnements virtuels et artefacts lourds restent hors depot.

## Fichiers utiles

- `netlify.toml` : configuration Netlify depuis la racine du depot.
- `site/netlify.toml` : configuration equivalente si Netlify utilise directement `site/` comme base.
- `site/.nvmrc` : version Node recommandee.
- `site/public/_headers` : en-tetes de cache et de securite.
- `site/public/_redirects` : fallback SPA vers `index.html`.
- `.github/workflows/site-build.yml` : verification automatique du build sur GitHub.
- `.gitattributes` : normalisation des fins de ligne et marquage des fichiers binaires.

## Reglages Netlify attendus

Avec le fichier `netlify.toml` a la racine, Netlify peut lire directement :

- Base directory : `site`
- Build command : `npm run build`
- Publish directory : `dist`
- Node.js : `22`

Si ces champs sont saisis manuellement dans l'interface Netlify, garder les memes valeurs.

## Avant publication

1. Regenerer `site/public/data/*.json` si `bd/fusion_ieee.db` a change.
2. Verifier que le build frontend passe.
3. Verifier que les fichiers lourds ne sont pas ajoutes au depot.
4. Publier le projet sur GitHub via l'interface de ton choix.
5. Connecter le depot GitHub dans Netlify.

## Donnees publiees

Le site publie les donnees contenues dans `site/public/data/`. C'est volontaire pour avoir un deploiement statique sans backend.

Les fichiers suivants doivent rester locaux :

- `bd/*.db`
- `analyse/**/*.pkl`
- `analyse/**/*.npz`
- `outputs/`
- `site/node_modules/`
- `site/dist/`
- `.venv/`
