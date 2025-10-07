import os
import sqlite3
from pathlib import Path

# Dossier contenant les bases
base_dir = Path("bd/")

# Liste tous les fichiers .sqlite ou .db
bases = [f for f in base_dir.glob("*.sqlite")] + [f for f in base_dir.glob("*.db")]

def inspect_db(db_path):
    print("="*80)
    print(f"Base de données : {db_path.name}")
    print("="*80)
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Liste des tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [t[0] for t in cursor.fetchall()]
        
        if not tables:
            print("Aucune table trouvée.")
            return
        
        for table in tables:
            print(f"\nTable : {table}")
            # Structure
            cursor.execute(f"PRAGMA table_info({table});")
            columns = cursor.fetchall()
            
            print("Colonnes :")
            for col in columns:
                print(f"  - {col[1]} ({col[2]})")
            
            # Nombre d’enregistrements
            cursor.execute(f"SELECT COUNT(*) FROM {table};")
            count = cursor.fetchone()[0]
            print(f"Nombre d’enregistrements : {count}")
            
    except Exception as e:
        print(f"Erreur sur {db_path.name} : {e}")
    finally:
        conn.close()


# Parcours des bases
if not bases:
    print("Aucune base .sqlite ou .db trouvée dans le dossier.")
else:
    for db in bases:
        inspect_db(db)