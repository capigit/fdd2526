import pandas as pd
import joblib

# ------------------------------------------------
# 1) Charger le modèle
# ------------------------------------------------
model = joblib.load("analyse/prediction/prediction_model.joblib")
print("Modèle chargé.")

# ------------------------------------------------
# 2) Fonction de génération de features pour un nouvel article
# ------------------------------------------------
def extract_features(article):
    """
    article est un dictionnaire du type :
    {
        "title": "...",
        "authors": ["A", "B"],
        "abstract": "...",
        "keywords_clean": "machine learning data mining ..."
    }

    Dans la version actuelle, on ne garde que nb_auteurs
    parce que c'est la seule feature numérique du modèle.
    """
    
    nb_auteurs = 0
    if "authors" in article and isinstance(article["authors"], list):
        nb_auteurs = len(article["authors"])
    
    return pd.DataFrame([{
        "nb_auteurs": nb_auteurs
    }])


# ------------------------------------------------
# 3) Exemple de prédiction
# ------------------------------------------------
article_test = {
    "title": "Deep Learning for Sports Analytics",
    "authors": ["John Doe", "Alice Smith"],
    "abstract": "We explore predictive models applied to tracking data...",
    "keywords_clean": "deep learning tracking prediction models"
}

features = extract_features(article_test)

print("Features générées :", features)

# ------------------------------------------------
# 4) Prédiction du cluster
# ------------------------------------------------
pred_cluster = model.predict(features)[0]

print(f"Cluster prédit : {pred_cluster}")