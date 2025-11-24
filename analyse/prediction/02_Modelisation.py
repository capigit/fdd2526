"""
02_Modelisation.py
- Charge features.pkl
- Entraîne deux modèles de base : RandomForest et LinearSVC (option)
- Sauvegarde les modèles (joblib)
- Sauvegarde les résultats d'entraînement (scores sur train/test)
"""

import joblib
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import LinearSVC
from sklearn.metrics import accuracy_score, f1_score

OUT_DIR = r"G:/Mon Drive/Projets/FDD/analyse/prediction"

def main():
    data = joblib.load(f"{OUT_DIR}/features.pkl")
    df = data["df_features"]
    X = data["X"]
    le = data["label_encoder"]

    y = le.transform(df['country_clean'])

    # Train/test split (stratify on y)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2,
                                                        random_state=42, stratify=y)

    # 1) RandomForest
    rf = RandomForestClassifier(n_estimators=200, max_depth=25, n_jobs=-1, random_state=42)
    rf.fit(X_train, y_train)
    y_pred_train = rf.predict(X_train)
    y_pred_test = rf.predict(X_test)

    rf_train_acc = accuracy_score(y_train, y_pred_train)
    rf_test_acc = accuracy_score(y_test, y_pred_test)
    rf_test_f1 = f1_score(y_test, y_pred_test, average='weighted')

    # Sauvegarde
    joblib.dump(rf, f"{OUT_DIR}/model_country_rf.joblib")

    # 2) LinearSVC (rapide)
    svc = LinearSVC(max_iter=5000)
    svc.fit(X_train, y_train)
    y_pred_test_svc = svc.predict(X_test)

    svc_test_acc = accuracy_score(y_test, y_pred_test_svc)
    svc_test_f1 = f1_score(y_test, y_pred_test_svc, average='weighted')

    joblib.dump(svc, f"{OUT_DIR}/model_country_svc.joblib")

    # Résultats résumé
    results = {
        "rf_train_acc": rf_train_acc,
        "rf_test_acc": rf_test_acc,
        "rf_test_f1": rf_test_f1,
        "svc_test_acc": svc_test_acc,
        "svc_test_f1": svc_test_f1,
        "label_classes": le.classes_.tolist()
    }

    joblib.dump(results, f"{OUT_DIR}/models_results_summary.pkl")
    print("Modèles entraînés et sauvegardés. Résumé des scores :")
    print(results)

if __name__ == "__main__":
    main()