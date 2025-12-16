# Alpha-MarketAnalysis

A Python script to fetch freelance job data from Free-Work API and save it to Azure SQL Database.

## Installation

Install the required dependencies using pip:

```bash
pip install -r requirements.txt
```

## Configuration

Create a `.env` file in the project root with your Azure SQL Database credentials:

```
AZURE_SQL_SERVER=your-server.database.windows.net
AZURE_SQL_DATABASE=your-database
AZURE_SQL_USERNAME=your-username
AZURE_SQL_PASSWORD=your-password
```

## Usage

Run the script with Python:

```bash
python fetch_and_save.py
```

This will fetch job postings from the Free-Work API and save them to your Azure SQL Database.
