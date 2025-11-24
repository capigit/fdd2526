"""
03_Evaluation.py
- Charge modèles et features
- Produit rapport détaillé et matrice de confusion
- Sauvegarde: classification_report.txt, confusion_matrix.png, metrics.csv
"""

import joblib
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score

OUT_DIR = r"G:/Mon Drive/Projets/FDD/analyse/prediction"

def main():
    data = joblib.load(f"{OUT_DIR}/features.pkl")
    df = data["df_features"]
    X = data["X"]
    le = data["label_encoder"]

    # Charger modèle (RandomForest utilisé par défaut)
    rf = joblib.load(f"{OUT_DIR}/model_country_rf.joblib")

    # train/test split (same as in training)
    from sklearn.model_selection import train_test_split
    y = le.transform(df['country_clean'])
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2,
                                                        random_state=42, stratify=y)

    y_pred = rf.predict(X_test)

    acc = accuracy_score(y_test, y_pred)
    report = classification_report(y_test, y_pred, target_names=le.classes_, zero_division=0)
    cm = confusion_matrix(y_test, y_pred)

    # Sauvegarde rapport
    with open(f"{OUT_DIR}/classification_report_rf.txt", "w", encoding="utf-8") as f:
        f.write(f"Accuracy: {acc}\n\n")
        f.write(report)

    # Matrice de confusion (normalisée)
    cm_norm = cm.astype('float') / cm.sum(axis=1)[:, np.newaxis]
    plt.figure(figsize=(10,8))
    sns.heatmap(cm_norm, annot=False, cmap="Blues", xticklabels=le.classes_, yticklabels=le.classes_)
    plt.title("Confusion matrix (normalized) - RandomForest")
    plt.xlabel("Predicted")
    plt.ylabel("True")
    plt.xticks(rotation=45, ha='right')
    plt.yticks(rotation=0)
    plt.tight_layout()
    plt.savefig(f"{OUT_DIR}/confusion_matrix_rf.png", dpi=200)

    # Sauvegarde métriques sommaires
    metrics = {
        "accuracy": acc
    }
    joblib.dump(metrics, f"{OUT_DIR}/metrics_rf.pkl")

    print("Evaluation terminée. Accuracy:", acc)
    print("Fichiers sauvegardés dans", OUT_DIR)

if __name__ == "__main__":
    main()