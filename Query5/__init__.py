import logging
from py2neo import Graph
from py2neo.bulk import create_nodes, create_relationships
from py2neo.data import Node
import os
import pyodbc as pyodbc
import azure.functions as func

"""
Pour la requête 5, spécifiez et implémentez une API qui renvoie la durée moyenne des films
qui correspondent aux critères genre, acteur et directeur. L'interprétation de cette énoncé vous est laissée libre.
"""

def main(req: func.HttpRequest) -> func.HttpResponse:

    actor = req.params.get('actor')
    genre = req.params.get('genre')
    director = req.params.get('director')

    try:

        if not actor or not genre or not director:
            try:
                req_body = req.get_json()
            except ValueError:
                pass
            else:
                actor = req.params.get('actor') or req_body.get('actor')
                genre = req.params.get('genre') or req_body.get('genre')
                director = req.params.get('director') or req_body.get('director')

        neo4j_server = os.environ["TPBDD_NEO4J_SERVER"]
        neo4j_user = os.environ["TPBDD_NEO4J_USER"]
        neo4j_password = os.environ["TPBDD_NEO4J_PASSWORD"]

        if len(neo4j_server)==0 or len(neo4j_user)==0 or len(neo4j_password)==0:
            return func.HttpResponse("Au moins une des variables d'environnement n'a pas été initialisée.", status_code=500)

        errorMessage = ""
        result1 = []

        try:
            logging.info("Test de connexion avec py2neo...")
            graph = Graph(neo4j_server, auth=(neo4j_user, neo4j_password))
            filter1 = "n1.primaryName='{}'".format(actor) if actor else "true"
            filter2 = "n2.primaryName='{}'".format(director) if director else "true"
            producers = graph.run("MATCH (n1:Name)-[rel:ACTED_IN]->(t:Title)<-[rel2:DIRECTED]-(n2:Name) WITH DISTINCT t.tconst AS titleIds, n1, n2 WHERE {} AND {} RETURN titleIds".format(filter1, filter2))
            for producer in producers:
                result1.append(producer['titleIds'])
        except:
            errorMessage = "Erreur de connexion a la base Neo4j"

        if errorMessage != "":
            return func.HttpResponse(errorMessage)

        server = os.environ["TPBDD_SERVER"]
        database = os.environ["TPBDD_DB"]
        username = os.environ["TPBDD_USERNAME"]
        password = os.environ["TPBDD_PASSWORD"]
        driver= '{ODBC Driver 17 for SQL Server}'

        result2 = ""

        if len(server)==0 or len(database)==0 or len(username)==0 or len(password)==0:
            return func.HttpResponse("Au moins une des variables d'environnement n'a pas été initialisée.", status_code=500)

        try:
            logging.info("Test de connexion avec pyodbc...")
            filter3_1 = "JOIN tGenres ON tTitles.tconst = tGenres.tconst"
            filter3_2 = "tGenres.genre = '{}'".format(genre) if genre or "1=1"
            with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT SUM(tTitles.runtimeMinutes)/COUNT(*) FROM tTitles {} WHERE tTitles.tconst IN ({}) AND {}".format(filter3_1,str(result1).strip('[]'),filter3_2))
                rows = cursor.fetchall()
                for row in rows:
                    result2 += str(row[0])

        except:
            errorMessage = "Erreur de connection à la base SQL"

        if errorMessage != "":
            return func.HttpResponse(errorMessage)

        return func.HttpResponse(result2)

    except Exception as e:
        return func.HttpResponse(repr(e))

    

