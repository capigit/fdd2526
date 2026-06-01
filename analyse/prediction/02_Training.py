from pathlib import Path

import joblib
import pandas as pd
from scipy.sparse import load_npz
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
from sklearn.model_selection import train_test_split


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PREDICTION_DIR = PROJECT_ROOT / "analyse" / "prediction"


def main():
    X = load_npz(PREDICTION_DIR / "X_prediction.npz")
    y = pd.read_csv(PREDICTION_DIR / "y_prediction.csv")["cluster"]
    print("Loaded X:", X.shape)
    print("Loaded y:", y.shape)

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42,
        stratify=y,
    )

    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=30,
        min_samples_leaf=2,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    print("Accuracy:", accuracy_score(y_test, y_pred))
    print("\nClassification report:\n", classification_report(y_test, y_pred, zero_division=0))

    joblib.dump(model, PREDICTION_DIR / "rf_model_prediction.pkl")
    print("Modele sauvegarde : rf_model_prediction.pkl")


if __name__ == "__main__":
    main()
