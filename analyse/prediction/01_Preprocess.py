import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import joblib
import numpy as np

# Chargement des données
df = pd.read_csv("analyse/prediction/features_prediction.csv")

# On enlève les clusters trop petits pour éviter les erreurs
min_cluster_size = 2
cluster_counts = df['cluster'].value_counts()
valid_clusters = cluster_counts[cluster_counts >= min_cluster_size].index
df = df[df['cluster'].isin(valid_clusters)]

# Séparer features et target
X = df[['keyword', 'nb_auteurs', 'country', 'article_id']]
y = df['cluster']

# Préprocessing
preprocessor = ColumnTransformer(
    transformers=[
        ('tfidf', TfidfVectorizer(max_features=1000), 'keyword'),
        ('ohe', OneHotEncoder(handle_unknown='ignore'), ['country']),
        ('pass', 'passthrough', ['nb_auteurs'])
    ]
)

X_transformed = preprocessor.fit_transform(X)

# Sauvegarde
joblib.dump(preprocessor, "preprocessor.joblib")
np.save("analyse/prediction/X_features.npy", X_transformed)
np.save("analyse/prediction/y_labels.npy", y.values)

print("Prétraitement terminé.")
print(f"Shape final : {X_transformed.shape}")