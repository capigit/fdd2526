# Analyse d'articles scientifiques IEEE sur l'IA

Projet Python de fouille de donnees autour d'articles IEEE lies a l'intelligence artificielle, au machine learning, au deep learning, au NLP et aux LLM.

Le projet couvre toute la chaine :

- import des fichiers JSON IEEE vers SQLite ;
- fusion et harmonisation des bases ;
- analyse exploratoire ;
- clustering d'articles et d'auteurs ;
- analyse des collaborations ;
- entrainement d'un modele de prediction de cluster.

## Structure

```text
bdSource/             Fichiers JSON sources
bd/                   Bases SQLite generees
scripts_imports/      Import JSON vers SQLite
scripts_fusions/      Fusion et nettoyage des bases
inspectBdFusionnee/   Scripts de diagnostic de la base fusionnee
analyse/
  EDA/                Analyses exploratoires
  clustering/         TF-IDF, KMeans, preparation prediction
  prediction/         Pretraitement, entrainement, prediction
  collaboration/      Graphes de co-auteurs et communautes
outputs_eda/          Sorties CSV et images d'analyse
```

## Installation

Depuis la racine du projet :

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## Base fusionnee actuelle

La base principale est `bd/fusion_ieee.db`.

Etat observe :

- `articles` : 10 077 lignes
- `authors` : 53 493 lignes
- `labs` : 30 146 lignes
- `keywords` : 159 043 lignes
- `article_authors` : 57 162 lignes
- `author_labs` : 60 214 lignes

## Workflow complet

Les commandes peuvent etre lancees depuis la racine du projet.

### 1. Importer les JSON vers SQLite

Chaque script recree sa base cible dans `bd/`.

```bash
python scripts_imports/import_AI.py
python scripts_imports/import_DL.py
python scripts_imports/import_ML.py
python scripts_imports/import_NLP.py
python scripts_imports/import_ieee_to_sqlite.py
```

### 2. Fusionner et nettoyer

```bash
python scripts_fusions/fusion_bases_v2.py
python scripts_fusions/nettoyage_fusion.py
```

`fusion_bases_v2.py` remappe les IDs des articles, auteurs et laboratoires depuis les bases sources vers la base fusionnee. Cela evite les liens incorrects dans `article_authors` et `author_labs` apres fusion/dedoublonnage.

### 3. Inspecter la base

```bash
python liste_bd.py
python inspectBdFusionnee/descriptionBD.py
python inspectBdFusionnee/analyse_fusion.py
```

### 4. Analyses exploratoires

```bash
python analyse/EDA/01_Chargement_et_inspection.py
python analyse/EDA/02_Verification_integrite_relationnelle.py
python analyse/EDA/03_Statistiques_descriptives.py
python analyse/EDA/04_Analyse_mots_cles.py
python analyse/EDA/05_Analyse_auteurs_labs.py
python analyse/EDA/06_Qualite_des_donnees.py
```

Le script `03_Statistiques_descriptives.py` genere `analyse/EDA/stats_descriptives.csv`, utilise ensuite par le clustering.

### 5. Clustering

```bash
python analyse/clustering/01_Extraction_mots_cles_articles.py
python analyse/clustering/02_TFIDF_et_features.py
python analyse/clustering/03_Clustering_articles.py
python analyse/clustering/04_Clustering_auteurs.py
python analyse/clustering/Preparation_features_prediction.py.py
```

Sorties principales :

- `analyse/clustering/features.pkl`
- `analyse/clustering/features_auteurs.pkl`
- `analyse/clustering/clusters_articles.csv`
- `analyse/clustering/clusters_auteurs.csv`
- `analyse/prediction/features_prediction.csv`

### 6. Prediction

```bash
python analyse/prediction/01_Preprocess.py
python analyse/prediction/02_Training.py
python analyse/prediction/03_Predict.py
```

Le preprocessing sauvegarde aussi les transformateurs requis par la prediction :

- `tfidf_transformer.pkl`
- `ohe_country.pkl`
- `feature_config.pkl`

## Collaboration

Pour construire le graphe de co-auteurs et detecter les communautes :

```bash
python analyse/collaboration/01_Construction_graphe.py
python analyse/collaboration/02_Mesures_centralite.py
python analyse/collaboration/03_Detection_communautes.py
python analyse/collaboration/04_Visualisation_communautes.py
```

Une version HTML interactive peut etre generee avec :

```bash
python analyse/collaboration/graph_html.py
```

## Restitution web

Le dossier `site/` contient une application statique Vite + React + TypeScript destinee a Netlify.

Les donnees publiques du site sont exportees depuis `bd/fusion_ieee.db` vers `site/public/data/` :

```bash
python scripts_exports/export_site_data.py
```

Depuis `site/` :

```bash
npm install
npm run dev
npm run build
```

Deploiement Netlify :

- base directory : `site`
- build command : `npm run build`
- publish directory : `dist`

Le fichier `netlify.toml` situe a la racine donne ces parametres a Netlify. Le guide [DEPLOYMENT.md](DEPLOYMENT.md) resume les etapes de publication sans commande Git.

Checklist avant publication :

- regenerer `site/public/data/*.json` apres toute modification de `bd/fusion_ieee.db`
- lancer `npm run build` depuis `site/`
- verifier que `bd/*.db`, `analyse/**/*.pkl`, `analyse/**/*.npz`, `site/node_modules/` et `site/dist/` ne sont pas versionnes
- garder `site/public/data/` versionne si le site doit etre deploye sans pipeline de donnees cote Netlify
- tester localement `http://127.0.0.1:5173/` avant push

## Notes

- Les scripts ont ete rendus robustes aux chemins : ils deduisent la racine du projet depuis leur emplacement.
- Les bases SQLite et plusieurs fichiers de sortie sont des artefacts generes.
- Une sauvegarde locale de l'ancienne base fusionnee peut exister sous `bd/fusion_ieee.before_remap.db`.
- Le fichier `analyse/EDA/impact_publications.py` est actuellement vide.
- Le fichier `analyse/clustering/Preparation_features_prediction.py.py` conserve son nom actuel pour ne pas casser les commandes existantes.

## Licence

Projet distribue sous licence MIT.
