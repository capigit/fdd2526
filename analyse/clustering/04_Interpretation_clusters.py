import joblib
from collections import Counter

df_features, kmeans = joblib.load('G:/Mon Drive/Projets/FDD/analyse/clustering/kmeans_articles.pkl')
tfidf = joblib.load('G:/Mon Drive/Projets/FDD/analyse/clustering/features.pkl')[2]

feature_names = tfidf.get_feature_names_out()
centroids = kmeans.cluster_centers_

top_n = 10
for i, centroid in enumerate(centroids):
    top_indices = centroid[:len(feature_names)].argsort()[-top_n:][::-1]
    top_features = [feature_names[j] for j in top_indices]
    print(f"Cluster {i}: {', '.join(top_features)}")