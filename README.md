# YouTube Data Harvesting Project
This project aims to harvest data from YouTube using Python scripting and store it in a NoSQL (MongoDB) database as a data lake. The harvested data can then be fetched from the NoSQL database and migrated to a SQL (MySQL) database for further analysis. Additionally, SQL queries can be executed on the MySQL database to answer specific questions related to the uploaded channel information.

Prerequisites
Before running the scripts, make sure you have the following dependencies installed:

- Python (version X.X.X)
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

Fetch Uploaded Channel Information:

Run the function_to_fetch.py script.
Provide the necessary parameters (e.g., channel ID) as user input.
The script will fetch the uploaded channel information from the MongoDB database.
Migrate Data to MySQL:

Run the migrate_to_mysql.py script.
The script will migrate the fetched data from the MongoDB database to the MySQL database for further analysis.
Execute SQL Queries:

Run the execute_sql_queries.py script.
Write SQL queries in the script to answer specific questions related to the uploaded channel information.
The script will execute the SQL queries on the MySQL database and display the results
