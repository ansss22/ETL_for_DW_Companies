# Data Warehouse Automation Project

This project automates the creation and management of databases using Access, MySQL, and MongoDB. It includes scripts for creating database schemas, importing data from CSV files, and executing queries to analyze and manage data efficiently.

## Project Structure

- **`createDb_Access.py`**: Script for creating and managing an Access database.
- **`createDb_MySql.py`**: Script for setting up a MySQL database.
- **`createDb_MongoDB.py`**: Script for initializing a MongoDB database.
- **CSV Files**: Contain data to be imported into the databases.

## Features

- Automated table creation for Employee, Company, Performance, and Absences datasets.
- Data validation and import from structured CSV files.
- Predefined SQL queries for insights like:
  - Employee counts by company and country.
  - Average salary calculations.
  - Absence analysis by month.

## Requirements

- Python 3.7 or above
- Required libraries:
  - `pyodbc`
  - `pandas`
  - `mysql-connector-python` (for MySQL script)
  - `pymongo` (for MongoDB script)
- Microsoft Access ODBC Driver (for Access script)
- MySQL Server (for MySQL script)
- MongoDB Server (for MongoDB script)

## Setup

1. Clone the repository:

   ```bash
   git clone <repository_url>
   cd <repository_name>
   ```

2. Install required Python libraries:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### 1. Create Database and Tables

Run the appropriate script to create the database and tables:

- For Access:

  ```bash
  python createDb_Access.py
  ```

- For MySQL:

  ```bash
  python createDb_MySql.py
  ```

- For MongoDB:
  ```bash
  python createDb_MongoDB.py
  ```

### 2. Import Data

Each script includes functionality to import data from CSV files. Ensure the CSV files are correctly formatted and placed in the specified directory.

### 3. Execute Queries

The scripts execute predefined queries to analyze the data and display results. Modify or add queries in the script as needed.

## Notes

- Ensure the Access database file exists before running the `createDb_Access.py` script.
- Update connection strings and file paths in the scripts to match your environment.

## License

This project is open-source and available under the MIT License.

## Contributors

- **Anass El Amrany**
