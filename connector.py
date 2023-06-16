from pymongo import MongoClient
import json

client = MongoClient("mongodb://localhost:27017/") #Conexion con MongoDB
db = client["awards"] #Conexion a una base de datos
collection = db["Oscars"] #Conexion con la coleccion

if False:
   with open('oscars.json') as file:
      data = json.load(file)
   collection = db.Oscars
   collection.insert_many(data)


client.close()