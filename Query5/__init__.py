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

    if not actor or not genre or not director:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            actor = req.params.get('actor') or req_body.get('actor')
            genre = req.params.get('genre') or req_body.get('genre')
            director = req.params.get('director') or req_body.get('director')

    dataString = "actor : {} - genre : {} - director : {}".format(actor or "Not given", genre or "Not given", director or "Not given")

    func.HttpResponse(dataString)
    

