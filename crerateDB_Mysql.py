import mysql.connector
import pandas as pd

# Configuration de la base de données
host = 'localhost'  
database = '' #Nom de database
username = '' 
password = ''

# Chemins des fichiers CSV
csv_files = {
    "Employee": "chemin vers fichier Employee_Table.csv",
    "Company": "chemin vers fichier Company_Table.csv",
    "Performance": "chemin vers fichier Performance_Table.csv",
    "Absences": "chemin vers fichier Absences_Table.csv"
}

# Fonction pour vérifier si une table existe
def table_exists(cursor, table_name):
    try:
        cursor.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
        cursor.fetchall()  # Consommer les résultats
        return True
    except mysql.connector.Error:
        return False

# Fonction pour créer la base de données et les tables
def create_database_and_tables():
    connection = mysql.connector.connect(
        host=host,
        database=database,
        user=username,
        password=password
    )
    cursor = connection.cursor()

    # Requêtes pour créer les tables
    tables = {
        "Employee": '''CREATE TABLE Employee (
                        Employee_ID INT AUTO_INCREMENT PRIMARY KEY,
                        Name VARCHAR(100),
                        Age INT,
                        Sex VARCHAR(10),
                        Marital_Status VARCHAR(10),
                        Grade VARCHAR(10),
                        Company_ID INT,
                        Contract_Type VARCHAR(50),
                        Hire_Date DATE,
                        salary INT)''',
        "Performance": '''CREATE TABLE Performance (
                        Employee_ID INT,
                        Performance_Score FLOAT,
                        Performance_Bonus FLOAT,
                        FOREIGN KEY (Employee_ID) REFERENCES Employee(Employee_ID))''',
        "Company": '''CREATE TABLE Company (
                        Company_ID INT AUTO_INCREMENT PRIMARY KEY,
                        Company_Name VARCHAR(100),
                        Country VARCHAR(100))''',
        "Absences": '''CREATE TABLE Absences (
                        Employee_ID INT,
                        Absence_Days INT,
                        FOREIGN KEY (Employee_ID) REFERENCES Employee(Employee_ID))'''
    }

    # Création des tables
    for table_name, create_query in tables.items():
        if not table_exists(cursor, table_name):
            print(f"Création de la table {table_name}...")
            try:
                cursor.execute(create_query)
            except mysql.connector.Error as e:
                print(f"Erreur lors de la création de {table_name}: {e}")

    connection.commit()
    cursor.close()
    connection.close()
    print("Création des tables terminée.")

# Fonction pour importer les données depuis un fichier CSV
def import_csv_to_table(csv_path, table_name):
    df = pd.read_csv(csv_path, sep=';')

    # Conversion des dates si nécessaire
    if table_name == "Employee" and "Hire_Date" in df.columns:
        df['Hire_Date'] = pd.to_datetime(df['Hire_Date'], format='%d/%m/%Y %H:%M', errors='coerce')  # Conversion
        df['Hire_Date'] = df['Hire_Date'].dt.strftime('%Y-%m-%d')  # Reformater

    # Suppression des colonnes ID auto-incrémentées
    if table_name == "Employee" and "Employee_ID" in df.columns:
        df = df.drop(columns=["Employee_ID"])
    if table_name == "Company" and "Company_ID" in df.columns:
        df = df.drop(columns=["Company_ID"])

    connection = mysql.connector.connect(
        host=host,
        database=database,
        user=username,
        password=password
    )
    cursor = connection.cursor()

    # Correspondance des colonnes attendues
    expected_columns = {
        "Employee": ["Name", "Age", "Sex", "Marital_Status", "Grade", "Company_ID", "Contract_Type", "Hire_Date", "salary"],
        "Performance": ["Employee_ID", "Performance_Score", "Performance_Bonus"],
        "Company": ["Company_Name", "Country"],
        "Absences": ["Employee_ID", "Absence_Days"]
    }

    if set(df.columns) != set(expected_columns[table_name]):
        raise ValueError(f"Les colonnes du fichier CSV pour {table_name} ne correspondent pas aux colonnes attendues : {expected_columns[table_name]}")

    # Insertion des données dans la table
    for index, row in df.iterrows():
        placeholders = ", ".join(["%s"] * len(row))
        columns = ", ".join(df.columns)
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        try:
            cursor.execute(sql, tuple(row))
        except mysql.connector.Error as e:
            print(f"Erreur lors de l'insertion de la ligne {index + 1} dans {table_name}: {e}")
            continue

    connection.commit()
    cursor.close()
    connection.close()
    print(f"Données importées dans {table_name} depuis {csv_path}.")

# Fonction pour exécuter des requêtes SQL
# Fonction pour exécuter des requêtes SQL
def execute_queries():
    queries = [
        """
        SELECT 
            c.Country, 
            c.Company_Name, 
            COUNT(e.Employee_ID) AS total_employes, 
            AVG(e.Salary) AS salaire_moyen
        FROM Employee e
        INNER JOIN Company c ON e.Company_ID = c.Company_ID
        GROUP BY c.Country, c.Company_Name
        ORDER BY total_employes DESC, salaire_moyen DESC;
        """,
        """
        SELECT 
            COUNT(e.Employee_ID) AS total_employes
        FROM Employee e
        INNER JOIN Company c ON e.Company_ID = c.Company_ID
        WHERE c.Company_Name = 'Company_10'
        AND e.Hire_Date BETWEEN '2005-01-01' AND '2005-05-31';
        """,
        """
        SELECT 
            e.Sex, 
            e.Age, 
            e.Grade, 
            AVG(p.Performance_Bonus) AS bonus_moyennes
        FROM Employee e
        INNER JOIN Performance p ON e.Employee_ID = p.Employee_ID
        GROUP BY e.Sex, e.Age, e.Grade
        ORDER BY bonus_moyennes DESC;
        """,
        """
        SELECT 
            e.Employee_ID, 
            e.Sex, 
            e.Age, 
            e.Grade, 
            p.Performance_Bonus
        FROM Employee e
        INNER JOIN Performance p ON e.Employee_ID = p.Employee_ID
        ORDER BY p.Performance_Bonus DESC;
        """,
        """
        SELECT 
            MONTH(a.Absence_Days) AS mois, 
            SUM(a.Absence_Days) AS total_absence
        FROM Absences a
        GROUP BY mois
        ORDER BY total_absence DESC;
        """
    ]

    connection = mysql.connector.connect(
        host=host,
        database=database,
        user=username,
        password=password
    )
    cursor = connection.cursor()

    for i, query in enumerate(queries, start=1):
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            print(f"Résultats pour la requête {i}:")
            for row in rows:
                print(row)
            print("\n")
        except mysql.connector.Error as e:
            print(f"Erreur lors de l'exécution de la requête {i}: {e}")

    cursor.close()
    connection.close()
# Exécution du programme
create_database_and_tables()

for table_name, csv_path in csv_files.items():
    import_csv_to_table(csv_path, table_name)

execute_queries()
