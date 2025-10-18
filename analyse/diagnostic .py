import sqlite3
import pandas as pd

# Connexion à la base
db_path = "bd/fusion_ieee.db"
conn = sqlite3.connect(db_path)

print("✅ Connexion réussie à la base :", db_path)

# Étape 1 : Liste des tables
tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn)
print("\n📋 Tables disponibles :")
print(tables)

# Étape 2 : Nombre de lignes par table
print("\n📊 Nombre de lignes par table :")
for t in tables["name"]:
    count = pd.read_sql(f"SELECT COUNT(*) AS total FROM {t}", conn)["total"][0]
    print(f"- {t} : {count}")

# Étape 3 : Vérification des doublons et valeurs manquantes
checks = {
    "articles": ["id", "title", "doi"],
    "authors": ["id", "name"],
    "labs": ["id", "lab_name"],
    "article_author": ["article_id", "author_id"],
    "author_labs": ["author_id", "lab_id"]
}

print("\n🔍 Vérification des doublons et valeurs manquantes :")
for table, cols in checks.items():
    try:
        df = pd.read_sql(f"SELECT * FROM {table}", conn)
        print(f"\n--- {table.upper()} ---")
        print("Nombre de lignes :", len(df))

        # Doublons
        dup = df[df.duplicated(subset=cols, keep=False)]
        print("→ Doublons :", len(dup))

        # Valeurs manquantes
        na = df.isnull().sum()
        print("→ Valeurs manquantes :")
        print(na[na > 0])
    except Exception as e:
        print(f"⚠️ Erreur sur {table} :", e)

# Étape 4 : Vérification de cohérence de liaison
print("\n🔗 Vérification de cohérence :")
try:
    missing_authors = pd.read_sql("""
        SELECT COUNT(*) AS nb
        FROM article_author aa
        LEFT JOIN authors a ON aa.author_id = a.id
        WHERE a.id IS NULL;
    """, conn)["nb"][0]
    print(f"Auteurs manquants dans article_author : {missing_authors}")

    missing_articles = pd.read_sql("""
        SELECT COUNT(*) AS nb
        FROM article_author aa
        LEFT JOIN articles ar ON aa.article_id = ar.id
        WHERE ar.id IS NULL;
    """, conn)["nb"][0]
    print(f"Articles manquants dans article_author : {missing_articles}")

    missing_labs = pd.read_sql("""
        SELECT COUNT(*) AS nb
        FROM author_labs al
        LEFT JOIN labs l ON al.lab_id = l.id
        WHERE l.id IS NULL;
    """, conn)["nb"][0]
    print(f"Labs manquants dans author_labs : {missing_labs}")

except Exception as e:
    print("⚠️ Erreur dans la vérification des liens :", e)

conn.close()
print("\n✅ Diagnostic terminé.")