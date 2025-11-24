import joblib
from sklearn.cluster import KMeans

df_features, X_final, tfidf, encoder = joblib.load('G:/Mon Drive/Projets/FDD/analyse/clustering/features.pkl')

# Clustering K-means
K = 5
kmeans = KMeans(n_clusters=K, random_state=42, n_init=10)
df_features['cluster'] = kmeans.fit_predict(X_final)

# Sauvegarde
import joblib
joblib.dump((df_features, kmeans), 'G:/Mon Drive/Projets/FDD/analyse/clustering/kmeans_articles.pkl')

# Nombre d'articles par cluster
print(df_features.groupby('cluster').size())