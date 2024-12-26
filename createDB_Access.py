import pyodbc
import pandas as pd
import os

# Chemin absolu du fichier Access
db_path = r"Chemin absolu du fichier Access"

# Connexion à la base de données Access
connection_string = f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={db_path};'

# Vérifier si une table existe
def table_exists(cursor, table_name):
    try:
        cursor.execute(f"SELECT * FROM {table_name} WHERE 1=0")
        return True
    except pyodbc.Error:
        return False

# Fonction pour créer les tables uniquement si elles n'existent pas
def create_database_and_tables():
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Le fichier {db_path} n'existe pas. Créez le fichier .accdb avant de continuer.")

    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()

    tables = {
        "Employee": '''CREATE TABLE Employee (
                        Employee_ID AUTOINCREMENT PRIMARY KEY,
                        Name TEXT,
                        Age INTEGER,
                        Sex TEXT,
                        Marital_Status TEXT,
                        Grade TEXT,
                        Company_ID INTEGER,
                        Contract_Type TEXT,
                        Hire_Date DATE,
                        salary INTEGER)''',
        "Performance": '''CREATE TABLE Performance (
                        Employee_ID INTEGER,
                        Performance_Score DOUBLE,
                        Performance_Bonus DOUBLE,
                        FOREIGN KEY (Employee_ID) REFERENCES Employee(Employee_ID))''',
        "Company": '''CREATE TABLE Company (
                        Company_ID AUTOINCREMENT PRIMARY KEY,
                        Company_Name TEXT,
                        Country TEXT)''',
        "Absences": '''CREATE TABLE Absences (
                        Employee_ID INTEGER,
                        Absence_Days INTEGER,
                        FOREIGN KEY (Employee_ID) REFERENCES Employee(Employee_ID))'''
    }

    for table_name, create_query in tables.items():
        if not table_exists(cursor, table_name):
            print(f"Création de la table {table_name}...")
            cursor.execute(create_query)

    connection.commit()
    cursor.close()
    connection.close()
    print("Vérification et création des tables terminées.")

# Fonction pour importer les données depuis les fichiers CSV
def import_csv_to_table(csv_path, table_name):
    df = pd.read_csv(csv_path, sep=';')

    if table_name == "Employee" and "Employee_ID" in df.columns:
        df = df.drop(columns=["Employee_ID"])
    if table_name == "Company" and "Company_ID" in df.columns:
        df = df.drop(columns=["Company_ID"])

    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()

    expected_columns = {
        "Employee": ["Name", "Age", "Sex", "Marital_Status", "Grade", "Company_ID", "Contract_Type", "Hire_Date", "salary"],
        "Performance": ["Employee_ID", "Performance_Score", "Performance_Bonus"],
        "Company": ["Company_Name", "Country"],
        "Absences": ["Employee_ID", "Absence_Days"]
    }

    if table_name not in expected_columns:
        raise ValueError(f"Table {table_name} n'est pas reconnue.")
    
    if set(df.columns) != set(expected_columns[table_name]):
        raise ValueError(f"Les colonnes du fichier CSV pour {table_name} ne correspondent pas aux colonnes attendues : {expected_columns[table_name]}")

    for index, row in df.iterrows():
        placeholders = ", ".join(["?"] * len(row))
        columns = ", ".join(df.columns)
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        try:
            cursor.execute(sql, tuple(row))
        except pyodbc.Error as e:
            print(f"Erreur lors de l'insertion de la ligne {index + 1} dans {table_name}: {e}")
            continue

    connection.commit()
    cursor.close()
    connection.close()
    print(f"Données importées dans {table_name} depuis {csv_path}.")

# Fonction pour exécuter une requête SQL et afficher les résultats
def execute_query(query, connection, cursor):
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        if rows:
            for row in rows:
                print(row)
        else:
            print("No results returned.")
    except pyodbc.Error as e:
        print(f"Error executing query: {e}")

# Fonction pour exécuter les requêtes SQL
def execute_queries():
    queries = [
        {
            "title": "Total Employés dans 'Company_10' (entre janvier et mai 2005)",
            "query": """
                SELECT 
                    COUNT(Employee.Employee_ID) AS total_employes
                FROM 
                    Employee
                INNER JOIN 
                    Company
                ON 
                    Employee.Company_ID = Company.Company_ID
                WHERE 
                Company.Company_Name = 'Company_10'
                AND FORMAT(Employee.Hire_Date, 'yyyy') = '2005'
                AND FORMAT(Employee.Hire_Date, 'mm') BETWEEN '01' AND '05';
            """
        },
        {
            "title": "Employés par Pays et Par Entreprise (Salaire Moyen)",
            "query": """
                SELECT Company.Company_name, Company.Country, COUNT(Employee.Employee_id) AS total_employes, AVG(Employee.salary) AS salaire_moyen
                FROM Employee INNER JOIN Company ON Employee.Company_ID = Company.Company_Id
                GROUP BY Company.Company_name, Company.Country
                ORDER BY COUNT(Employee.Employee_id) DESC , AVG(Employee.salary) DESC;
            """
        },
        {
            "title": "Employés par Pays et Entreprise (Total Employés, Salaire Moyen)",
            "query": """
                SELECT Company.country, Company.Company_Name, Count(Employee.Employee_ID) AS total_employes, Avg(Employee.salary) AS salary
                FROM Employee INNER JOIN Company ON Employee.Company_ID = Company.Company_ID
                GROUP BY Company.country, Company.Company_Name
                ORDER BY Count(Employee.Employee_ID) DESC , Avg(Employee.salary) DESC;
            """
        },
        {
            "title": "Total des Absences par Mois",
            "query": """
                SELECT MONTH(Absences.Absence_Days) AS mois, SUM(Absences.Absence_Days) AS total_absence
                FROM Absences
                GROUP BY MONTH(Absences.Absence_Days)
                ORDER BY SUM(Absences.Absence_Days) DESC;
            """
        },
        # {
        #     "title": "query 4 ",
        #     "query": """
        #          SELECT e.Employee_ID, e.Sex, e.Age, e.Grade, p.Performance_Bonus
        #          FROM Employee AS e INNER JOIN Performance AS p ON e.Employee_ID = p.Employee_ID
        #          ORDER BY p.Performance_Bonus DESC;
        #     """
        # }
        
    ]
    
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()

    # Exécuter et afficher les résultats pour chaque requête
    for query_info in queries:
        print(f"Executing Query: {query_info['title']}")  # Afficher le titre de la requête
        execute_query(query_info['query'], connection, cursor)  # Exécuter la requête
        print("\n")

    cursor.close()
    connection.close()

# Fonction principale
def main():
    create_database_and_tables()

    import_csv_to_table(r"chemin vers fichier Emloyee_Table.csv", "Employee")
    import_csv_to_table(r"chemin vers fichier Performance_Table.csv", "Performance")
    import_csv_to_table(r"chemin vers fichier Company_Table.csv", "Company")
    import_csv_to_table(r"chemin vers fichier Absences_Table.csv", "Absences")

    execute_queries()

if __name__ == "__main__":
    main()
