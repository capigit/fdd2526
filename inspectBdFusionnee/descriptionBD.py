import sqlite3

db_path = "bd/fusion_ieee.db"   # chemin vers ta base

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# récupérer toutes les tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()

for table in tables:
    table_name = table[0]
    print(f"\n=== Structure de la table : {table_name} ===")
    cursor.execute(f"PRAGMA table_info('{table_name}');")
    for col in cursor.fetchall():
        cid, name, col_type, notnull, default, pk = col
        print(f" - {name} ({col_type})  | NOT NULL={notnull} | PK={pk} | DEFAULT={default}")

conn.close()