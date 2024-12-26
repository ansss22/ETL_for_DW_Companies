from pymongo import MongoClient
import pandas as pd

# Configuration de MongoDB
mongo_host = "localhost" # MongoDB
mongo_port = 27017 # Port par défaut
mongo_db_name = "dw_companies" # Nom de la base de données

# Chemins des fichiers CSV
csv_files = {
    "Employee": "chemin vers le fichier Emlpyee_Table.csv",
    "Company": "chemin vers le fichier Company_Table.csv",
    "Performance": "chemin vers le fichier Performance_Table.csv",
    "Absences": "chemin vers le fichier Absences_Table.csv"
}

# Connexion à MongoDB
client = MongoClient(mongo_host, mongo_port)
db = client[mongo_db_name]

# Fonction pour importer un fichier CSV dans une collection MongoDB
def import_csv_to_mongo(csv_path, collection_name):
    df = pd.read_csv(csv_path, sep=';')

    # Conversion des dates si nécessaire
    if collection_name == "Employee" and "Hire_Date" in df.columns:
        df['Hire_Date'] = pd.to_datetime(df['Hire_Date'], format='%d/%m/%Y %H:%M', errors='coerce')

    # Conversion des données en dictionnaires
    data = df.to_dict(orient='records')

    # Insertion des données dans la collection
    collection = db[collection_name]
    try:
        collection.insert_many(data)
        print(f"Données importées dans la collection {collection_name} depuis {csv_path}.")
    except Exception as e:
        print(f"Erreur lors de l'insertion dans {collection_name}: {e}")

# Fonction pour afficher les résultats dans un tableau lisible
def display_results(title, query_result):
    print(f"\n{title}\n{'-' * len(title)}")
    if query_result:
        df = pd.DataFrame(query_result)
        print(df.to_string(index=False))  # Affichage sans index
    else:
        print("Aucun résultat trouvé.")

# Fonction pour exécuter des requêtes MongoDB
def execute_queries():
     # Query 1 : Total des employés et salaire moyen par pays et entreprise
    query1 = list(db["Employee"].aggregate([
        {"$lookup": {
            "from": "Company",
            "localField": "Company_ID",
            "foreignField": "Company_ID",
            "as": "CompanyInfo"
        }},
        {"$unwind": "$CompanyInfo"},
        {"$group": {
            "_id": {"Country": "$CompanyInfo.Country", "Company_Name": "$CompanyInfo.Company_Name"},
            "total_employes": {"$sum": 1},
            "salaire_moyen": {"$avg": "$Salary"}
        }},
        {"$sort": {"total_employes": -1, "salaire_moyen": -1}}
    ]))
    print("\nRequête 1 : Total des employés et salaire moyen par pays et entreprise")
    for doc in query1:
        salaire_moyen = doc['salaire_moyen']
        salaire_moyen_str = f"{round(salaire_moyen, 2)}" if salaire_moyen is not None else "Non disponible"
        print(f"Pays: {doc['_id']['Country']}, Entreprise: {doc['_id']['Company_Name']}, "
              f"Total Employés: {doc['total_employes']}, Salaire Moyen: {salaire_moyen_str}")

    # Query 2 : Nombre d'employés embauchés par une entreprise spécifique durant une période donnée
    query2 = list(db["Employee"].aggregate([
        {"$lookup": {
            "from": "Company",
            "localField": "Company_ID",
            "foreignField": "Company_ID",
            "as": "CompanyInfo"
        }},
        {"$unwind": "$CompanyInfo"},
        {"$match": {
            "CompanyInfo.Company_Name": "Company_10",
            "Hire_Date": {"$gte": pd.Timestamp("2005-01-01"), "$lte": pd.Timestamp("2005-05-31")}
        }},
        {"$group": {"_id": None, "total_employes": {"$sum": 1}}}
    ]))
    print("\nRequête 2 : Nombre d'employés embauchés entre janvier et mai 2005 dans Company_10")
    if query2:
        print(f"Total employés : {query2[0]['total_employes']}")
    else:
        print("Aucun employé trouvé dans cette période.")

    # Query 3 : Moyenne des bonus de performance par sexe, âge et grade
    query3 = list(db["Employee"].aggregate([
        {"$lookup": {
            "from": "Performance",
            "localField": "Employee_ID",
            "foreignField": "Employee_ID",
            "as": "PerformanceInfo"
        }},
        {"$unwind": "$PerformanceInfo"},
        {"$group": {
            "_id": {"Sex": "$Sex", "Age": "$Age", "Grade": "$Grade"},
            "bonus_moyennes": {"$avg": "$PerformanceInfo.Performance_Bonus"}
        }},
        {"$sort": {"bonus_moyennes": -1}}
    ]))
    print("\nRequête 3 : Moyenne des bonus de performance par sexe, âge et grade")
    for doc in query3:
        print(f"Sexe: {doc['_id']['Sex']}, Âge: {doc['_id']['Age']}, Grade: {doc['_id']['Grade']}, "
              f"Bonus Moyen: {round(doc['bonus_moyennes'], 2)}")

    # Query 4 : Liste des employés et de leur bonus de performance
    query4 = list(db["Employee"].aggregate([
        {"$lookup": {
            "from": "Performance",
            "localField": "Employee_ID",
            "foreignField": "Employee_ID",
            "as": "PerformanceInfo"
        }},
        {"$unwind": "$PerformanceInfo"},
        {"$project": {
            "Employee Name": "$Name",
            "Sex": 1,
            "Age": 1,
            "Grade": 1,
            "Performance Bonus": "$PerformanceInfo.Performance_Bonus"
        }},
        {"$sort": {"Performance Bonus": -1}}
    ]))
    print("\nRequête 4 : Liste des employés et de leur bonus de performance")
    for doc in query4:
        print(f"Nom: {doc['Employee Name']}, Sexe: {doc['Sex']}, Âge: {doc['Age']}, "
              f"Grade: {doc['Grade']}, Bonus: {doc['Performance Bonus']}")

    # Query 5 : Total des jours d'absence par mois
    query5 = list(db["Absences"].aggregate([
        {"$project": {
            "mois": {"$month": "$Absence_Date"},
            "total_absence": "$Absence_Days"
        }},
        {"$group": {
            "_id": "$mois",
            "total_absence": {"$sum": "$total_absence"}
        }},
        {"$sort": {"total_absence": -1}}
    ]))
    print("\nRequête 5 : Total des jours d'absence par mois")
    for doc in query5:
        print(f"Mois: {doc['_id']}, Total Absences: {doc['total_absence']}")



# Importation des données
for collection_name, csv_path in csv_files.items():
    import_csv_to_mongo(csv_path, collection_name)

# Exécution des requêtes
execute_queries()
