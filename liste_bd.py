import os
import sqlite3

# Dossier contenant les bases de données
BASE_DIR = "bd"  # <-- à adapter si besoin

def lister_tables(db_path):
    """Retourne la liste des tables d'une base SQLite donnée."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        return tables
    except sqlite3.Error as e:
        print(f"Erreur avec la base {db_path} : {e}")
        return []

def parcourir_bases(base_dir):
    """Parcourt tous les fichiers .db dans un dossier et affiche leurs tables."""
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.endswith(".db"):
                db_path = os.path.join(root, file)
                print(f"\n📂 Base de données : {db_path}")
                tables = lister_tables(db_path)
                if tables:
                    for t in tables:
                        print(f"   - {t}")
                else:
                    print("   (Aucune table trouvée ou base vide)")

if __name__ == "__main__":
    parcourir_bases(BASE_DIR)