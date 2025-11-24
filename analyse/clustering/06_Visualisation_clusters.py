import joblib
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt

df_features, kmeans = joblib.load('G:/Mon Drive/Projets/FDD/analyse/clustering/kmeans_articles.pkl')
X_keywords = joblib.load('G:/Mon Drive/Projets/FDD/analyse/clustering/features.pkl')[1]

# PCA 2D
pca = PCA(n_components=2, random_state=42)
X_pca = pca.fit_transform(X_keywords.toarray())

plt.figure(figsize=(10,6))
K = df_features['cluster'].nunique()
for i in range(K):
    plt.scatter(X_pca[df_features['cluster']==i,0], X_pca[df_features['cluster']==i,1], label=f'Cluster {i}', alpha=0.5)

plt.xlabel("PCA 1")
plt.ylabel("PCA 2")
plt.title("Clusters d'articles (TF-IDF + K-means)")
plt.legend()
plt.show()