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

def getFilmIdsByActorDirector(actor, director):

    if not actor and not director:
        return ([], "")

    neo4j_server = os.environ["TPBDD_NEO4J_SERVER"]
    neo4j_user = os.environ["TPBDD_NEO4J_USER"]
    neo4j_password = os.environ["TPBDD_NEO4J_PASSWORD"]

    errorMessage = ""
    result = []
    code = 200

    if len(neo4j_server)==0 or len(neo4j_user)==0 or len(neo4j_password)==0:
        errorMessage = "Au moins une des variables d'environnement n'a pas été initialisée."
        code = 500
        return (result, errorMessage, code)

    try:
        logging.info("Test de connexion avec py2neo...")
        graph = Graph(neo4j_server, auth=(neo4j_user, neo4j_password))
        filter1 = "n1.primaryName='{}'".format(actor) if actor else "true"
        filter2 = "n2.primaryName='{}'".format(director) if director else "true"
        producers = graph.run("MATCH (n1:Name)-[rel:ACTED_IN]->(t:Title)<-[rel2:DIRECTED]-(n2:Name) WITH DISTINCT t.tconst AS titleIds, n1, n2 WHERE {} AND {} RETURN titleIds".format(filter1, filter2))
        for producer in producers:
            result.append(str(producer['titleIds']))
    except:
        errorMessage = "Erreur de connexion a la base Neo4j"
        code = 500

    if len(result1) == 0:
        errorMessage = "Pas de films trouvés"
        code = 404

    return (result, errorMessage, code)


def getAverageDurationByGenre(genre, filmIds):
    server = os.environ["TPBDD_SERVER"]
    database = os.environ["TPBDD_DB"]
    username = os.environ["TPBDD_USERNAME"]
    password = os.environ["TPBDD_PASSWORD"]
    driver= '{ODBC Driver 17 for SQL Server}'

    result = 0.
    errorMessage = ""
    code = 200

    if len(server)==0 or len(database)==0 or len(username)==0 or len(password)==0:
        errorMessage = "Au moins une des variables d'environnement n'a pas été initialisée."
        code = 500
        return (result, errorMessage, code)

    try:
        logging.info("Test de connexion avec pyodbc...")
        filter3_1 = "JOIN tGenres ON tTitles.tconst = tGenres.tconst" if genre else ""
        filter3_2 = "tGenres.genre = '{}'".format(genre) if genre else "1=1"
        filter3_3 = "tTitles.tconst IN ({})".format(str(result1).strip('[]')) if len(filmIds) > 0 else "1=1"
        logging.info("SELECT SUM(tTitles.runtimeMinutes), COUNT(*) FROM tTitles {} WHERE {} AND {}".format(filter3_1,filter3_3,filter3_2))
        with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT SUM(tTitles.runtimeMinutes), COUNT(*) FROM tTitles {} WHERE {} AND {}".format(filter3_1,filter3_3,filter3_2))
            rows = cursor.fetchall()
            (duration, count) = rows[0]

            if float(count) == 0.:
                errorMessage = "Pas de films trouvés"
                code = 404
            else:
                result = float(duration) / float(count)

    except:
        errorMessage = "Erreur de connection à la base SQL"
        code = 500

    return (result, errorMessage, code)


def parseParams(req):
    actor = req.params.get('actor')
    genre = req.params.get('genre')
    director = req.params.get('director')

    if not actor or not genre or not director:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            actor = req.params.get('actor') or req_body.get('actor')
            genre = req.params.get('genre') or req_body.get('genre')
            director = req.params.get('director') or req_body.get('director')

    return (actor, director, genre)


def main(req: func.HttpRequest) -> func.HttpResponse:

    (actor, director, genre) = parseParams(req)

    (filmIds, errorMessage, code) = getFilmIdsByActorDirector(actor, director)

    if code != 200:
        return func.HttpResponse(errorMessage, status_code=code)

    (averageDuration, errorMessage, code) = getAverageDurationByGenre(genre, filmIds)

    if errorMessage:
        return func.HttpResponse(errorMessage, status_code=code)

    result = "Average Film Duration : {}".format(averageDuration)    

    return func.HttpResponse(result, status_code=code)

    

