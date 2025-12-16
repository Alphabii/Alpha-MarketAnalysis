import json
import os
import requests
import pyodbc
from dotenv import load_dotenv

load_dotenv()


def create_table_if_not_exists(cursor):
    """Create the freelance_jobs table if it doesn't exist"""
    create_table_query = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='freelance_jobs' AND xtype='U')
    CREATE TABLE freelance_jobs (
        id INT PRIMARY KEY,
        created_at DATETIME2,
        job NVARCHAR(MAX) NOT NULL,
        job_slug NVARCHAR(MAX) NOT NULL,
        slug NVARCHAR(MAX) NOT NULL,
        title NVARCHAR(MAX) NOT NULL,
        skills NVARCHAR(MAX),
        soft_skills NVARCHAR(MAX),
        company_name NVARCHAR(MAX),
        city NVARCHAR(MAX),
        long FLOAT,
        lat FLOAT,
        duration NVARCHAR(100),
        remote NVARCHAR(50),
        max_tjm INT,
        min_tjm INT,
        experience NVARCHAR(50),
        description NVARCHAR(MAX),
        candidate_profile NVARCHAR(MAX),
        company_description NVARCHAR(MAX)
    );
    """
    cursor.execute(create_table_query)
    cursor.connection.commit()


def save_to_db(data_json):
    server = os.getenv("AZURE_SQL_SERVER", "server-azure-sql-db.database.windows.net")
    database = os.getenv("AZURE_SQL_DATABASE", "db_dev")
    username = os.getenv("AZURE_SQL_USERNAME", "user_readwrite")
    password = os.getenv("AZURE_SQL_PASSWORD")
    driver = '{ODBC Driver 18 for SQL Server}'
    
    connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30;'
    
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()
    
    create_table_if_not_exists(cursor)

    insert_query = """
        IF NOT EXISTS (SELECT 1 FROM freelance_jobs WHERE id = ?)
        BEGIN
            INSERT INTO freelance_jobs (
                id, 
                created_at, 
                job, 
                job_slug,
                slug,
                title,
                skills,
                soft_skills,
                company_name,
                city,
                long,
                lat,
                duration, 
                remote, 
                max_tjm, 
                min_tjm, 
                experience, 
                description, 
                candidate_profile, 
                company_description
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
        END
    """

    for data in data_json:
        id = data["id"]
        created_at = data["createdAt"]
        job = data["job"]["name"]
        job_slug = data["job"]["slug"]
        slug = data["slug"]
        title = data["title"]
        skills = json.dumps([skill["name"] for skill in data["skills"]])
        soft_skills = json.dumps([skill["name"] for skill in data["softSkills"]])
        company_name = data["company"]["name"]
        city = data["location"]["shortLabel"]
        long = data["location"]["longitude"]
        lat = data["location"]["latitude"]
        duration_value = (
            data["durationValue"] if data["durationValue"] is not None else ""
        )
        duration_period = (
            data["durationPeriod"] if data["durationPeriod"] is not None else ""
        )
        if duration_value and duration_period:
            duration = str(duration_value) + " " + duration_period
        else:
            duration = None
        remote = data["remoteMode"]
        max_tjm = data["maxDailySalary"]
        min_tjm = data["minDailySalary"]
        experience = data["experienceLevel"]
        description = data["description"]
        candidate_profile = data["candidateProfile"]
        company_description = data["companyDescription"]

        cursor.execute(
            insert_query,
            (
                id,  # for the EXISTS check
                id,
                created_at,
                job,
                job_slug,
                slug,
                title,
                skills,
                soft_skills,
                company_name,
                city,
                long,
                lat,
                duration,
                remote,
                max_tjm,
                min_tjm,
                experience,
                description,
                candidate_profile,
                company_description,
            ),
        )

    print(len(data_json), " rows inserted")

    connection.commit()
    cursor.close()
    connection.close()


def fetch_all_data():
    response = requests.get(
        "https://www.free-work.com/api/job_postings/count?contracts=contractor",
    )
    number_jobs = response.json()
    for page in range(1, number_jobs // 300 + 2):
        print("Fetching page", page)
        fetch_data(page, 300)


def fetch_data(page, items_per_page):
    response = requests.get(
        "https://www.free-work.com/api/job_postings?contracts=contractor&order=date&page={}&itemsPerPage={}".format(
            page, items_per_page
        ),
        headers={"Accept": "application/json"},
    )
    save_to_db(response.json())


def main():
    # fetch_all_data()
    fetch_data(1, 250)


if __name__ == "__main__":
    main()
