from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from sqlalchemy import create_engine
import pymongo
import mysql.connector

#=======================GOOGLE API KEY=================================
api_key = 'AIzaSyB8QccKYXYdlnVZxhPHJ6sQ3aeJYVih-Do'
youtube = build('youtube', 'v3', developerKey=api_key)


#======================MONGODB CONNECTION==============================
client = pymongo.MongoClient("mongodb+srv://sridhar15:HeyramSridhar@cluster0.cdr7vsk.mongodb.net/?retryWrites=true&w=majority")
database = client["youtube"]
collection = database["channel_information"]

#=====================SQLALCHEMY FOR SEND AS DATAFRAME================
engine = create_engine('mysql+mysqlconnector://root:Sridhar15@localhost/youtube_data')


#=====================MYSQL - PYTHON CONNECTION=======================
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="Sridhar15",
  database="youtube_data"
)
mycursor = mydb.cursor()
