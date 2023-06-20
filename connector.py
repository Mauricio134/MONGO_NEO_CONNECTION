from pymongo import MongoClient
import json
from neo4j import GraphDatabase
from bson import ObjectId

client = MongoClient("mongodb://localhost:27017/") # Conexion con MongoDB
db = client["awards"] # Conexion a una base de datos
collection = db["Oscars"] # Conexion con la colección

# Configurar la conexión a Neo4j
neo4j_driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "mauricioapaza"))
neo4j_session = neo4j_driver.session()

if False:
    with open('oscars.json') as file:
        data = json.load(file)
    collection = db.Oscars
    collection.insert_many(data)

result = collection.find({"year_film": 1927})

first_item = True
first_node_id = None

for item in result:
    year_film = item["year_film"]
    year_ceremony = item["year_ceremony"]
    ceremony = item["ceremony"]
    category = item["category"]
    name = item["name"]
    film = item["film"]
    winner = item["winner"]
    
    if first_item:
         record = neo4j_session.run("CREATE (p:Principal {year_film: $year_film, year_ceremony: $year_ceremony, ceremony: $ceremony, category: $category, name: $name, film: $film, winner: $winner})",
                          year_film=year_film,
                          year_ceremony=year_ceremony,
                          ceremony=ceremony,
                          category=category,
                          name=name,
                          film=film,
                          winner=winner)
         first_item = False
    else:
         record = neo4j_session.run("CREATE (p:Pelicula {year_film: $year_film, year_ceremony: $year_ceremony, ceremony: $ceremony, category: $category, name: $name, film: $film, winner: $winner})",
                          year_film=year_film,
                          year_ceremony=year_ceremony,
                          ceremony=ceremony,
                          category=category,
                          name=name,
                          film=film,
                          winner=winner)
         # Crear relación con el primer nodo basado en year_film
         cypher_query = "MATCH (a:Principal {year_film: $first_year_film}), (b:Pelicula {year_film: $current_year_film}) MERGE (a)-[:SE_ESTRENO_JUNTO_A]->(b)"
         neo4j_session.run(cypher_query, first_year_film=year_film, current_year_film=1927)

a = input("Quieres Continuar: ")
if a == 'y' :
   cypher_query = "MATCH (n) DETACH DELETE n"
   neo4j_session.run(cypher_query)
client.close()
neo4j_driver.close()
