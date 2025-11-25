# FDD - Fouille de Données

## 📌 Titre du Projet

**FDD (Framework for Data-Driven Discovery)** est un système intégré d'analyse exploratoire, de clustering et de détection de communautés appliqué à des articles scientifiques en IA, Machine Learning, Deep Learning et NLP provenant de la base de données IEEE.

---

## 📖 Description et Objectifs

### Description
Ce projet vise à exploiter les données bibliographiques publiées par IEEE pour conduire une **analyse scientifique multidimensionnelle** : exploration des tendances de recherche, identification de domaines thématiques, analyse des collaborations entre auteurs et prédiction d'attributs.

### Objectifs Principaux
1. **Intégration de données** : Fusionner plusieurs bases de données JSON provenant d'IEEE en une base SQLite unique et harmonisée
2. **Analyse exploratoire (EDA)** : Caractériser la qualité, les lacunes et les tendances des données
3. **Clustering d'articles** : Identifier des domaines thématiques via TF-IDF et K-means
4. **Clustering d'auteurs** : Regrouper les chercheurs par domaine d'expertise
5. **Analyse des collaborations** : Construire un graphe de co-publication et détecter les communautés scientifiques
6. **Prédiction** : Modéliser le pays du laboratoire principal en fonction des mots-clés
7. **Visualisation** : Produire des graphiques et des réseaux pour interpréter les résultats

### Cas d'Usage
- Cartographier l'écosystème de recherche en IA/ML/NLP
- Identifier les tendances émergentes et les domaines dominants
- Analyser les réseaux de collaboration entre chercheurs et institutions
- Prédire les attributs manquants d'articles scientifiques

---

## 🔧 Prérequis et Environnement

### Système d'Exploitation
- **Windows 10/11** (testé avec Windows PowerShell 5.1)
- **Linux/macOS** (compatible avec shell standard)

### Python
- **Python 3.8+** (recommandé : Python 3.9 ou 3.10)
- Téléchargeable depuis [python.org](https://www.python.org/downloads/)

### Bibliothèques Python Requises
```
pandas          # Manipulation et analyse de données
scikit-learn    # Machine Learning (clustering, TF-IDF, encodage)
networkx        # Analyse de graphes et réseaux
python-louvain  # Détection de communautés
wordcloud       # Visualisation de nuages de mots
matplotlib      # Visualisation de données
numpy           # Calculs numériques
scipy           # Calculs scientifiques
unidecode       # Suppression des accents et normalization de texte
```

### Ressources Matérielles
- **Disque dur** : ~2 GB minimum pour les bases de données et résultats
- **RAM** : 4 GB minimum (8 GB recommandé pour le clustering grand-scale)
- **GPU** : Non requis (recommandé pour de très grands datasets)

### Outils Spécifiques
- **SQLite** : Intégré à Python (sqlite3), pas d'installation supplémentaire
- **Jupyter Notebook** : Optionnel mais recommandé pour l'exploration interactive
- **Gephi** : Optionnel pour la visualisation avancée du graphe de collaboration

---

## 📦 Installation et Configuration

### Étape 1 : Cloner le Projet

```bash
# Windows (PowerShell)
cd "G:\Mon Drive\Projets"
git clone https://github.com/capigit/fdd2526.git
cd FDD
```

### Étape 2 : Créer et Activer un Environnement Virtuel Python

#### Sur Windows (PowerShell)
```powershell
# Créer l'environnement virtuel
python -m venv venv

# Activer l'environnement
.\venv\Scripts\Activate.ps1
```

#### Sur Linux/macOS
```bash
python3 -m venv venv
source venv/bin/activate
```

### Étape 3 : Installer les Dépendances

```bash
# Mettre à jour pip
pip install --upgrade pip

# Installer les packages requis
pip install -r requirements.txt
```

### Étape 4 : Vérifier l'Installation

```bash
# Tester l'import de pandas
python -c "import pandas; print(f'pandas {pandas.__version__} OK')"

# Tester l'import de scikit-learn
python -c "from sklearn import __version__; print(f'sklearn OK')"

# Tester l'import de networkx
python -c "import networkx as nx; print(f'networkx {nx.__version__} OK')"
```

### Étape 5 : Configuration Optionnelle

Si vous utilisez Jupyter Notebook :
```bash
pip install jupyter
```

Pour visualiser les graphes dans Gephi, téléchargez [Gephi](https://gephi.org/) (gratuit).

---

## 🚀 Instructions pour Exécuter le Projet

### Vue d'Ensemble du Flux d'Exécution

Le projet suit un pipeline de traitement linéaire :

```
1. Import des données (scripts_imports/)
    ↓
2. Fusion des bases (scripts_fusions/)
    ↓
3. Nettoyage (scripts_fusions/nettoyage_fusion.py)
    ↓
4. Analyse exploratoire (analyse/EDA/)
    ↓
5. Clustering (analyse/clustering/)
    ↓
6. Analyse de collaborations (analyse/collaboration/)
    ↓
7. Prédiction (analyse/prediction/)
```

### Étape 1 : Importer les Données IEEE

Les fichiers JSON bruts se trouvent dans `bdSource/`. Le script d'import remplit les bases SQLite individuelles.

```bash
# Exemple : Importer les données AI
cd scripts_imports
python import_AI.py

# Importer tous les domaines (à faire pour chaque)
python import_DL.py
python import_ML.py
python import_NLP.py
```

**Résultat attendu** : Création de `bd/ieee_*.db` pour chaque domaine.

### Étape 2 : Fusionner les Bases

```bash
cd scripts_fusions
python fusion_bases_v2.py
```

**Résultat attendu** : Création de `bd/fusion_ieee.db` avec ~9000+ articles et 15000+ auteurs.

### Étape 3 : Nettoyer et Corriger les Doublons

```bash
python nettoyage_fusion.py
```

**Résultat attendu** : Suppression des doublons dans `labs`, `authors`, et `article_authors`.

### Étape 4 : Analyse Exploratoire (EDA)

```bash
cd ../../analyse/EDA
python 01_Chargement_et_inspection.py
python 02_Verification_integrite_relationnelle.py
python 03_Statistiques_descriptives.py
python 04_Analyse_mots_cles.py
python 05_Analyse_auteurs_labs.py
python 06_Qualite_des_donnees.py
```

**Résultat attendu** :
- Fichiers CSV dans `outputs_eda/` (summary_counts.csv, top10_authors.csv, etc.)
- Affichage des statistiques de base
- Visualisation des distributions

### Étape 5 : Clustering d'Articles et d'Auteurs

```bash
cd ../clustering
python 01_Extraction_mots_cles_articles.py
python 02_TFIDF_et_features.py
python 03_Clustering_articles.py
python 04_Interpretation_clusters.py
python 05_Clustering_auteurs.py
python 06_Visualisation_clusters.py
```

**Résultat attendu** :
- Fichier `df_keywords_grouped.csv`
- Clusters d'articles (2D PCA)
- Clusters d'auteurs (2D PCA)
- Interprétation des thèmes par cluster

### Étape 6 : Analyse des Collaborations

```bash
cd ../collaboration
python 01_Construction_graphe.py
python 02_Mesures_centralite.py
python 03_Detection_communautes.py
python 04_Visualisation_communautes.py
```

**Résultat attendu** :
- `graphe_collaboration.gexf` (importable dans Gephi)
- `centrality.csv` (mesures de centralité)
- `communities.csv` (communautés détectées)
- Graphique du réseau en 2D

### Étape 7 : Prédiction

```bash
cd ../prediction
python 01_Construction_features.py
python 02_Modelisation.py
python 03_Evaluation.py
```

**Résultat attendu** :
- Modèles ML sauvegardés
- Rapport de performance (précision, recall, F1-score)
- Matrice de confusion

### Exemple Complet depuis zéro

```bash
# Activation de l'environnement (si pas déjà activé)
.\venv\Scripts\Activate.ps1  # Windows
# source venv/bin/activate    # Linux/macOS

# Fusion complète
cd scripts_fusions
python fusion_bases_v2.py
python nettoyage_fusion.py

# EDA rapide
cd ../analyse/EDA
python 01_Chargement_et_inspection.py

# Clustering complet
cd ../clustering
for script in 01_Extraction_mots_cles_articles.py 02_TFIDF_et_features.py 03_Clustering_articles.py
do
    python $script
done

# Analyse collaboration
cd ../collaboration
python 01_Construction_graphe.py
python 02_Mesures_centralite.py
```

---

## 📁 Structure des Fichiers et Répertoires

```
FDD/
│
├── README.md                          # Ce fichier
├── requirements.txt                   # Dépendances Python
├── liste_bd.py                        # Script pour lister les tables SQLite
├── diagnostic.ipynb                   # Notebook Jupyter de diagnostic
├── ProjetFDD.ipynb                    # Notebook Jupyter principal (Google Colab)
│
├── 📂 bd/                             # Bases de données SQLite
│   ├── fusion_ieee.db                 # Base fusionnée principale (~150 MB)
│   └── ieee_*.db                      # Bases par domaine (AI, DL, ML, NLP)
│
├── 📂 bdSource/                       # Fichiers JSON bruts IEEE (source)
│   ├── IEEE_artificial_intelligence_1825.json
│   ├── IEEE_deep_learning_1825.json
│   ├── IEEE_llm_Journals_1825.json
│   ├── IEEE_machine_learning_1825.json
│   └── IEEE_NLP_Journals_1825.json
│
├── 📂 scripts_imports/                # Scripts d'import JSON → SQLite
│   ├── import_AI.py
│   ├── import_DL.py
│   ├── import_ieee_to_sqlite.py       # Script générique d'import
│   ├── import_ML.py
│   └── import_NLP.py
│
├── 📂 scripts_fusions/                # Scripts de fusion et nettoyage
│   ├── fusion_bases.py                # Version v1 (dépréciée)
│   ├── fusion_bases_v2.py             # Version v2 (actuellement utilisée)
│   ├── inspect_bd.py                  # Script d'inspection
│   └── nettoyage_fusion.py            # Correction doublons et intégrité
│
├── 📂 inspectBdFusionnee/             # Scripts d'inspection de la base
│   ├── analyse_fusion.py
│   ├── analyseExploratoire.ipynb
│   └── diagnostic.py
│
├── 📂 analyse/                        # Analyses principales
│
│   ├── 📂 EDA/                        # Exploratory Data Analysis
│   │   ├── 01_Chargement_et_inspection.py
│   │   ├── 02_Verification_integrite_relationnelle.py
│   │   ├── 03_Statistiques_descriptives.py
│   │   ├── 04_Analyse_mots_cles.py
│   │   ├── 05_Analyse_auteurs_labs.py
│   │   └── 06_Qualite_des_donnees.py
│   │
│   ├── 📂 clustering/                 # Clustering d'articles et auteurs
│   │   ├── 01_Extraction_mots_cles_articles.py
│   │   ├── 02_TFIDF_et_features.py
│   │   ├── 03_Clustering_articles.py
│   │   ├── 04_Interpretation_clusters.py
│   │   ├── 05_Clustering_auteurs.py
│   │   ├── 06_Visualisation_clusters.py
│   │   └── df_keywords_grouped.csv    # Output : Mots-clés groupés par article
│   │
│   ├── 📂 collaboration/              # Analyse de collaborations (graphes)
│   │   ├── 01_Construction_graphe.py
│   │   ├── 02_Mesures_centralite.py
│   │   ├── 03_Detection_communautes.py
│   │   ├── 04_Visualisation_communautes.py
│   │   ├── centrality.csv             # Output : Mesures de centralité
│   │   ├── communities.csv            # Output : Communautés détectées
│   │   └── graphe_collaboration.gexf  # Output : Graphe (Gephi)
│   │
│   └── 📂 prediction/                 # Prédiction d'attributs
│       ├── 01_Construction_features.py
│       ├── 02_Modelisation.py
│       └── 03_Evaluation.py
│
├── 📂 outputs_eda/                    # Outputs de l'EDA
│   ├── articles_per_year.csv
│   ├── articles_without_authors_sample.csv
│   ├── articles_without_keywords_sample.csv
│   ├── authors_per_country.csv
│   ├── summary_counts.csv
│   ├── top10_authors.csv
│   ├── top10_labs.csv
│   └── top20_keywords.csv
│
├── 📂 venv/                           # Environnement virtuel Python (généré)
│
└── .gitignore                         # Fichiers à ignorer par Git
```

### Description Détaillée des Répertoires

| Dossier | Description |
|---------|-----------|
| **bd/** | Contient les bases SQLite créées. `fusion_ieee.db` est la base principale résultant de la fusion. |
| **bdSource/** | Fichiers JSON bruts téléchargés depuis IEEE. Source primaire des données. |
| **scripts_imports/** | Convertit les fichiers JSON en tables SQLite. À exécuter en premier. |
| **scripts_fusions/** | Fusionne les bases par domaine en une seule base, corrige les doublons. |
| **analyse/EDA/** | Charge et inspecte la base fusionnée, génère des statistiques descriptives. |
| **analyse/clustering/** | Applique TF-IDF, K-means, PCA pour regrouper articles et auteurs. |
| **analyse/collaboration/** | Construit un graphe de co-publication, détecte les communautés. |
| **analyse/prediction/** | Modélise et prédit des attributs (ex: pays du labo). |
| **outputs_eda/** | Fichiers CSV exportés depuis les analyses EDA. Prêts pour Excel ou visualisation. |

---

## 💡 Exemples d'Utilisation et Démonstrations

### Exemple 1 : Lister les Tables d'une Base

```python
import sqlite3
import pandas as pd

# Connexion
conn = sqlite3.connect('bd/fusion_ieee.db')

# Lister les tables
tables = pd.read_sql_query(
    "SELECT name FROM sqlite_master WHERE type='table'",
    conn
)
print("Tables disponibles:")
print(tables)

conn.close()
```

**Résultat attendu** :
```
       name
0  articles
1  authors
2  labs
3  keywords
4  article_authors
5  author_labs
```

### Exemple 2 : Récupérer les Top 10 Auteurs par Nombre d'Articles

```python
import sqlite3
import pandas as pd

conn = sqlite3.connect('bd/fusion_ieee.db')

query = """
SELECT 
    a.name AS author_name,
    COUNT(aa.article_id) AS nb_articles,
    COUNT(DISTINCT aa.article_id) AS nb_unique_articles
FROM authors a
JOIN article_authors aa ON a.id = aa.author_id
GROUP BY a.id
ORDER BY nb_articles DESC
LIMIT 10
"""

top_authors = pd.read_sql_query(query, conn)
print(top_authors)

conn.close()
```

### Exemple 3 : Analyser les Articles par Année

```python
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

conn = sqlite3.connect('bd/fusion_ieee.db')

query = """
SELECT 
    SUBSTR(date_publication, 1, 4) AS year,
    COUNT(*) AS nb_articles
FROM articles
WHERE date_publication IS NOT NULL
GROUP BY year
ORDER BY year
"""

articles_by_year = pd.read_sql_query(query, conn)

# Visualisation
plt.figure(figsize=(12, 5))
plt.plot(articles_by_year['year'], articles_by_year['nb_articles'], marker='o')
plt.xlabel('Année')
plt.ylabel('Nombre d\'articles')
plt.title('Évolution du nombre d\'articles publiés')
plt.grid(True, alpha=0.3)
plt.show()

conn.close()
```

### Exemple 4 : Clustering Simple d'Articles par Mots-clés

```python
import sqlite3
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans

# Charger les mots-clés groupés
df = pd.read_csv('analyse/clustering/df_keywords_grouped.csv')

# TF-IDF
vectorizer = TfidfVectorizer(max_features=100)
X = vectorizer.fit_transform(df['keyword'].fillna(''))

# K-Means
kmeans = KMeans(n_clusters=5, random_state=42)
df['cluster'] = kmeans.fit_predict(X)

# Afficher la distribution
print(df['cluster'].value_counts().sort_index())
```

### Exemple 5 : Charger le Graphe de Collaboration et Analyser les Hubs

```python
import networkx as nx
import pandas as pd

# Charger le graphe GEXF
G = nx.read_gexf('analyse/collaboration/graphe_collaboration.gexf')

# Top 10 nœuds par degré
degrees = dict(G.degree())
top_nodes = sorted(degrees.items(), key=lambda x: x[1], reverse=True)[:10]

print("Top 10 nœuds par degré (nombre de co-auteurs):")
for node, degree in top_nodes:
    print(f"  {node}: {degree}")

# Nombre de communautés
num_components = nx.number_connected_components(G)
print(f"\nNombre de composantes connexes : {num_components}")
```

---

## 🎓 Conseils et Bonnes Pratiques

### Pour les Développeurs

1. **Versioning et Git**
   - Committez régulièrement vos modifications
   - Utilisez des messages de commit clairs : `git commit -m "feat: add clustering visualization"`
   - Branchez pour les grandes expériences : `git checkout -b feature/new-analysis`

2. **Structure du Code**
   - Un script = une analyse logique
   - Nommez les variables clairement (ex: `df_keywords_grouped` plutôt que `d1`)
   - Ajoutez des docstrings pour les fonctions complexes

3. **Gestion des Données**
   - Gardez les fichiers bruts dans `bdSource/` intact
   - Générez les résultats dans des fichiers séparés (`outputs/`)
   - Versionnez les petits fichiers CSV, ignorez les gros fichiers (.db, .gexf)

4. **Performance**
   - Pour de grandes tables, utilisez les index SQLite
   - Filtrez les données avec SQL plutôt que pandas quand possible
   - Utilisez `sparse=True` dans scikit-learn pour les matrices TF-IDF

5. **Documentation**
   - Commentez les portions non-évidentes
   - Décrivez les paramètres des algorithmes (ex: `n_clusters=5, why?`)
   - Mettez à jour ce README si vous ajoutez des étapes ou scripts

### Pour les Utilisateurs

1. **Avant de Lancer le Pipeline Complet**
   - Testez chaque étape isolément
   - Vérifiez les outputs attendus
   - Gardez une copie de secours de `bd/fusion_ieee.db`

2. **Exploration Interactive**
   - Utilisez `diagnostic.ipynb` ou `ProjetFDD.ipynb` pour l'exploration
   - Modifiez les requêtes SQL pour répondre à vos questions
   - Exécutez les cellules progressivement, ne lancez pas tout d'un coup

3. **Gestion des Résultats**
   - Exportez les outputs en CSV pour traitement externe
   - Sauvegardez les graphiques en PNG ou PDF
   - Utilisez Gephi pour la visualisation avancée des réseaux

4. **Dépannage**
   - Vérifiez que `bd/fusion_ieee.db` existe
   - Assurez-vous que l'environnement virtuel est activé (`venv/Scripts/Activate`)
   - Vérifiez les versions des packages : `pip list`
   - Consultez les logs d'erreur complètement avant de relancer

### Erreurs Courantes

| Erreur | Cause | Solution |
|--------|-------|----------|
| `ModuleNotFoundError: No module named 'pandas'` | pandas n'est pas installé | `pip install pandas` |
| `sqlite3.OperationalError: no such table: articles` | Base non fusionnée | Lancez `fusion_bases_v2.py` |
| `MemoryError` lors du clustering | Dataset trop grand | Réduisez `max_features` dans TF-IDF |
| `.gexf` ne s'ouvre pas dans Gephi | Format incompatible | Régénérez avec `01_Construction_graphe.py` |

---

## 📜 Licence et Crédits

### Licence
Ce projet est fourni à titre de travail universitaire/recherche. Voir le fichier `LICENSE` pour les details.

### Données Sources
- **Fournisseur** : IEEE (Institut des Ingénieurs Électriciens et Électroniciens)
- **Domaines couverts** : Artificial Intelligence, Deep Learning, Machine Learning, Natural Language Processing
- **Période** : Environ 1825 articles par domaine (~9000+ articles au total)
- **Conditions d'utilisation** : Conformes aux conditions d'utilisation d'IEEE. Usage éducatif/recherche uniquement.

### Auteurs et Contributeurs
- **Auteur principal** : Equipe FDD2526
- **Dépôt GitHub** : https://github.com/capigit/fdd2526

### Remerciements
- NetworkX pour la manipulation de graphes
- Scikit-learn pour les algorithmes ML
- Community Detection (python-louvain) pour la détection de communautés
- Pandas pour la manipulation de données

### Contact
Pour toute question ou contribution, veuillez :
1. Ouvrir une issue sur GitHub
2. Soumettre une pull request avec vos améliorations

---

## 🔗 Ressources Additionnelles

### Documentation
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [Scikit-learn User Guide](https://scikit-learn.org/stable/user_guide.html)
- [NetworkX Tutorial](https://networkx.org/documentation/stable/tutorial.html)
- [SQLite Python](https://docs.python.org/3/library/sqlite3.html)

### Tutoriels Recommandés
- [K-means Clustering Explained](https://scikit-learn.org/stable/modules/clustering.html#k-means)
- [TF-IDF for Text Analysis](https://scikit-learn.org/stable/modules/feature_extraction.html#tfidf-term-weighting)
- [Community Detection in Graphs](https://python-louvain.readthedocs.io/)

### Configuration VS Code (Optionnel)

Créez `.vscode/settings.json` pour une meilleure expérience :
```json
{
    "python.defaultInterpreterPath": "${workspaceFolder}/venv/bin/python",
    "python.linting.enabled": true,
    "python.linting.pylintEnabled": true,
    "[python]": {
        "editor.formatOnSave": true,
        "editor.defaultFormatter": "ms-python.python"
    }
}
```

---

## 📝 Version et Historique

| Version | Date | Notes |
|---------|------|-------|
| 1.0 | Nov 2025 | README initial complet avec tous les détails d'installation et d'utilisation |

**Dernière mise à jour** : 25 novembre 2025

---

## ❓ FAQ (Questions Fréquemment Posées)

**Q1 : Puis-je exécuter un seul script sans lancer le pipeline complet ?**
R : Oui ! Chaque script est conçu pour être indépendant. Assurez-vous simplement que les données dont il a besoin existent (ex: `fusion_ieee.db` pour les analyses).

**Q2 : Combien de temps prend le pipeline complet ?**
R : Environ 10-30 minutes selon votre machine :
- Import : 2-5 min
- Fusion : 1 min
- EDA : 5 min
- Clustering : 10-15 min
- Collaboration : 5-10 min

**Q3 : Puis-je utiliser un autre format de base (PostgreSQL, MongoDB, etc.) ?**
R : Actuellement, le projet utilise SQLite. Une migration nécessiterait de modifier les scripts d'import et les requêtes SQL.

**Q4 : Les résultats du clustering sont-ils reproductibles ?**
R : Partiellement. Nous utilisons `random_state=42` pour K-Means, mais la détection de communautés (Louvain) contient du non-déterminisme. Pour une reproductibilité complète, fixez la seed NumPy.

**Q5 : Comment puis-je ajouter mes propres données ?**
R : Créez un fichier JSON au format IEEE (ou créez un script d'import personnalisé) et ajoutez-le à `bdSource/`.

---