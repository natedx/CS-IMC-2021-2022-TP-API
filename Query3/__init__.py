import logging
from py2neo import Graph
from py2neo.bulk import create_nodes, create_relationships
from py2neo.data import Node
import os
import pyodbc as pyodbc
import azure.functions as func

"""
Neo4j :
Montrez les artistes ayant eu plusieurs responsabilités dans un même film
(ex: à la fois acteur et directeur, ou toute autre combinaison) et les titres de ces films.
"""

def main(req: func.HttpRequest) -> func.HttpResponse:

    neo4j_server = os.environ["TPBDD_NEO4J_SERVER"]
    neo4j_user = os.environ["TPBDD_NEO4J_USER"]
    neo4j_password = os.environ["TPBDD_NEO4J_PASSWORD"]

    if len(neo4j_server)==0 or len(neo4j_user)==0 or len(neo4j_password)==0:
        return func.HttpResponse("Au moins une des variables d'environnement n'a pas été initialisée.", status_code=500)

    errorMessage = ""
    dataString = ""

    try:
        logging.info("Test de connexion avec py2neo...")
        graph = Graph(neo4j_server, auth=(neo4j_user, neo4j_password))
        producers = graph.run("MATCH (name:Name)-[rel]->(title:Title) WITH DISTINCT name AS distinctNames, title, count(DISTINCT type(rel)) AS rels WHERE rels > 1 RETURN distinctNames.primaryName")
        for producer in producers:
            dataString += f"CYPHER: primaryName={producer['distinctNames.primaryName']}\n"
    except:
        errorMessage = "Erreur de connexion a la base Neo4j"

    if errorMessage != "":
        return func.HttpResponse(dataString + errorMessage, status_code=500)

    else:
        return func.HttpResponse(dataString)
    
