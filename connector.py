from pymongo import MongoClient
import json
from neo4j import GraphDatabase
import os

client = MongoClient("mongodb://localhost:27017/")
db = client["awards"]
collection = db["Oscars"]

neo4j_driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "mauricioapaza"))
neo4j_session = neo4j_driver.session()

def choose2():
    print("<------ESCOGER COMO ELIMINAR----->")
    print("1) ELIMINAR 1 DATO.")
    print("2) ELIMINAR TODOS LOS DATOS.")
    return int(input("ELEGIR: "))

def choose1():
    print("<------ESCOGER QUE REALIZAR------>")
    print("1) ELIMINAR DATO.")
    print("2) INSERTAR DATO.")
    print("3) SEGUIR CONSULTA.")
    return int(input("ELEGIR: "))

if False:
    with open('oscars.json') as file:
        data = json.load(file)
    collection = db.Oscars
    collection.insert_many(data)
    #Gloria Swanson
#result = collection.find({"year_film": 1927})
#result = collection.aggregate([{"$limit": 10}])
first_item = True

run = True

while run:
    q = input("INGRESA TU QUERY: ")
    query = eval(q)
    result = collection.find(query)
    q = input("INGRESA TU AGGREGATE: ")
    query2 = eval(q)
    result = collection.aggregate(query2)
    os.system('cls' if os.name == 'nt' else 'clear')
    c = choose1()
    os.system('cls' if os.name == 'nt' else 'clear')
    if c == 1:
        d = choose2()
        q = input("INGRESA TU QUERY: ")
        query3 = eval(q)
        os.system('cls' if os.name == 'nt' else 'clear')
        if d == 1:
            collection.delete_one(query3)
        elif d == 2:
            collection.delete_many({})
        #result = collection.find(query)
        #result = collection.aggregate(query2)
    elif c == 2:
        year_film = int(input("INGRESAR LA FECHA DE LA PELICULA: "))
        year_ceremony = int(input("INGRESAR LA FECHA DE LA PREMIACION: "))
        ceremony = int(input("INGRESAR LA CEREMONIA: "))
        category = str(input("INGRESAR LA CATEGORIA: "))
        name = str(input("INGRESAR EL NOMBRE DEL ACTOR: "))
        film = str(input("INGRESAR EL NOMBRE DE LA PELICULA: "))
        winner = str(input("INGRESAR SI GANO O NO: "))
        os.system('cls' if os.name == 'nt' else 'clear')
        doc = { 'year_film': year_film, 'year_ceremony': year_ceremony, 'ceremony': ceremony, 'category':category, 'name': name, 'film': film, 'winner': winner}
        collection.insert_one(doc)
        #result = collection.find(query)
        #result = collection.aggregate(query2)
    else:
        print("NO HAY CAMBIOS...")
        os.system('cls' if os.name == 'nt' else 'clear')

    for item in result:
        year_film = item["year_film"]
        year_ceremony = item["year_ceremony"]
        ceremony = item["ceremony"]
        category = item["category"]
        name = item["name"]
        film = item["film"]
        winner = item["winner"]
        
        if first_item:
            record = neo4j_session.run("CREATE (p:Principal {year_film: $year_film, year_ceremony: $year_ceremony, ceremony: $ceremony, category: $category, nombre: $nombre, film: $film, winner: $winner, name: $display_name}) RETURN p",
                            year_film=year_film,
                            year_ceremony=year_ceremony,
                            ceremony=ceremony,
                            category=category,
                            nombre=name,
                            film=film,
                            winner=winner,
                            display_name=film + ' - ' + name)  # Concatena film y name en display_name
            first_item = False
        else:
            record = neo4j_session.run("CREATE (p:Pelicula {year_film: $year_film, year_ceremony: $year_ceremony, ceremony: $ceremony, category: $category, nombre: $nombre, film: $film, winner: $winner, name: $display_name}) RETURN p",
                            year_film=year_film,
                            year_ceremony=year_ceremony,
                            ceremony=ceremony,
                            category=category,
                            nombre=name,
                            film=film,
                            winner=winner,
                            display_name=film + ' - ' + name)  # Concatena film y name en display_name
            cypher_query = "MATCH (a:Principal {name: $first_name}), (b:Pelicula {name: $current_name}) MERGE (a)-[:PARTICIPO_TAMBIEN_EN]->(b)"
            neo4j_session.run(cypher_query, first_name=name, current_name="Louise Dresser")
        
    a = input("¿DESEAS CONTINUAR? ")
    if a == 'y':
        cypher_query = "MATCH (n) DETACH DELETE n"
        neo4j_session.run(cypher_query)
    elif a == 'n':
        cypher_query = "MATCH (n) DETACH DELETE n"
        neo4j_session.run(cypher_query)
        run = False
    os.system('cls' if os.name == 'nt' else 'clear')

client.close()
neo4j_driver.close()
