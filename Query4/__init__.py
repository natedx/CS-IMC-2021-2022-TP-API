import logging

import azure.functions as func

"""
SQL :
Les genres pour lesquels au moins un film a une même personne qui a été la fois directeur et acteur
(ex: si Alice a été acteur à la fois directeur dans une comédie,
et que Bob a été à la fois acteur et directeur dans un film d'action alors il faut renvoyer [comédie, action])
"""

def main(req: func.HttpRequest) -> func.HttpResponse:
    server = os.environ["TPBDD_SERVER"]
    database = os.environ["TPBDD_DB"]
    username = os.environ["TPBDD_USERNAME"]
    password = os.environ["TPBDD_PASSWORD"]
    driver= '{ODBC Driver 17 for SQL Server}'

    if len(server)==0 or len(database)==0 or len(username)==0 or len(password)==0:
        return func.HttpResponse("Au moins une des variables d'environnement n'a pas été initialisée.", status_code=500)

    try:
        logging.info("Test de connexion avec pyodbc...")
        with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT([dbo].[tGenres].genre) FROM [dbo].[tNames] JOIN [dbo].[tPrincipals] ON [dbo].[tNames].nconst = [dbo].[tPrincipals].nconst JOIN [dbo].[tTitles] ON [dbo].[tPrincipals].tconst = [dbo].[tTitles].tconst JOIN [dbo].[tGenres] ON [dbo].[tTitles].tconst = [dbo].[tGenres].tconst GROUP BY primaryName, [dbo].[tNames].nconst, primaryTitle, [dbo].[tTitles].tconst, [dbo].[tGenres].genre HAVING COUNT(DISTINCT category)>1;")
            rows = cursor.fetchall()
            for row in rows:
                dataString += f"SQL: genre={row[0]}\n"


    except:
        errorMessage = "Erreur de connexion a la base SQL"

    if errorMessage != "":
        return func.HttpResponse(dataString + errorMessage, status_code=500)

    else:
        return func.HttpResponse(dataString)
