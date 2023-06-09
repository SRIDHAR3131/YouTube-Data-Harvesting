# YouTube Data Harvesting and Warhousing
This project aims to harvest data from YouTube using Python scripting and store it in a NoSQL (MongoDB) database as a data lake. The harvested data can then be fetched from the NoSQL database and migrated to a SQL (MySQL) database for further analysis. Additionally, SQL queries can be executed on the MySQL database to answer specific questions related to the uploaded channel information.

Prerequisites
Before running the scripts, make sure you have the following dependencies installed:

- Python 3.9 or later
- MongoDB
- MySQL

Configure the MongoDB and MySQL database connection:

Open the  database.py file and update the MongoDB and MySQL connection details.
Make sure you have a MongoDB database and collection created to store the harvested data.
Create a MySQL database and tables to store the migrated data.

Harvest YouTube Data:

Run the Youtube.py script.
Provide the channel ID as user input.
The script will fetch the channel data from YouTube using the YouTube Data API and store it in the MongoDB database as a data lake.

## Upload Channel Information:
![Screenshot 2023-06-09 002919](https://github.com/SRIDHAR3131/YouTube-Data-Harvesting/assets/68391060/0d3d096b-11f1-42e8-8dad-9668adacef79)

Run the function_upload_fetch.py script.
Provide the necessary parameters (e.g., channel ID) as user input.
The script will fetch the uploaded channel information from the MongoDB database.

## Migrate Data to MySQL:
![Screenshot 2023-06-09 001705](https://github.com/SRIDHAR3131/YouTube-Data-Harvesting/assets/68391060/ca1890a2-cf35-4d8e-b288-eec8a8cf30c2)

Run the migrate_to_mysql.py script.
The script will migrate the fetched data from the MongoDB database to the MySQL database for further analysis.

## Execute SQL Queries:
![Screenshot 2023-06-09 004012](https://github.com/SRIDHAR3131/YouTube-Data-Harvesting/assets/68391060/d61a59a1-afcd-4947-ac96-99f6d223d65b)

Run the execute_sql_queries.py script.
Write SQL queries in the script to answer specific questions related to the uploaded channel information.
The script will execute the SQL queries on the MySQL database and display the results
