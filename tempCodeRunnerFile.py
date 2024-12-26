import pyodbc
import pandas as pd
import os

# Chemin absolu du fichier Access
db_path = r"C:\Users\VOSTRO 3500\OneDrive\Bureau\IDAA\S3\Bi_Bigdata\TP_TD\TDTP10. Model_Etoil_flacon\DW_Companies2.accdb"

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
        """
        SELECT Company.Country, Company.Company_Name, COUNT(Employee.Employee_ID) AS total_employes,
        AVG(Employee.Salary) AS salaire_moyen
            FROM Employee INNER JOIN Company ON Employee.Company_ID = Company.Company_ID 
            GROUP BY Company.Country, Company.Company_Name 
            ORDER BY COUNT(Employee.Employee_ID) DESC, AVG(Employee.Salary)
        """,
        """
        SELECT COUNT(Employee.Employee_ID) AS total_employes 
        FROM Employee INNER JOIN Company ON Employee.Company_ID = Company.Company_ID 
        WHERE Company.Company_Name = 'Company_10' AND Employee.Hire_Date BETWEEN #2005-01-01# AND #2005-05-31#;
        """,
        """
        SELECT Employee.Sex, Employee.Age, Employee.Grade, AVG(Performance.Performance_Bonus) AS bonus_moyennes 
        FROM Employee INNER JOIN Performance ON Employee.Employee_ID = Performance.Employee_ID 
        GROUP BY Employee.Sex, Employee.Age, Employee.Grade 
        ORDER BY AVG(Performance.Performance_Bonus) DESC;
        """,
        """
        SELECT Employee.Employee_ID, Employee.Sex, Employee.Age, Employee.Grade, Performance.Performance_Bonus 
        FROM Employee INNER JOIN Performance ON Employee.Employee_ID = Performance.Employee_ID 
        ORDER BY Performance.Performance_Bonus DESC;
        """,
        """
        SELECT MONTH(Absences.Absence_Date) AS mois, SUM(Absences.Absence_Data) AS total_absence 
        FROM Absences GROUP BY MONTH(Absences.Absence_Date) 
        ORDER BY SUM(Absences.Absence_Days) DESC;
        """
    ]
    
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()

    # Execute and print results for each query
    for query in queries:
        print("Executing Query:")
        execute_query(query, connection, cursor)
        print("\n")

    cursor.close()
    connection.close()

# Fonction principale
def main():
    create_database_and_tables()

    import_csv_to_table(r"C:\Users\VOSTRO 3500\OneDrive\Bureau\IDAA\S3\Bi_Bigdata\TP_TD\TDTP10. Model_Etoil_flacon\Employee_Table.csv", "Employee")
    import_csv_to_table(r"C:\Users\VOSTRO 3500\OneDrive\Bureau\IDAA\S3\Bi_Bigdata\TP_TD\TDTP10. Model_Etoil_flacon\Performance_Table.csv", "Performance")
    import_csv_to_table(r"C:\Users\VOSTRO 3500\OneDrive\Bureau\IDAA\S3\Bi_Bigdata\TP_TD\TDTP10. Model_Etoil_flacon\Company_Table.csv", "Company")
    import_csv_to_table(r"C:\Users\VOSTRO 3500\OneDrive\Bureau\IDAA\S3\Bi_Bigdata\TP_TD\TDTP10. Model_Etoil_flacon\Absences_Table.csv", "Absences")

    execute_queries()

if __name__ == "__main__":
    main()
