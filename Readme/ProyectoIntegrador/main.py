from reactivex import create
import csv
import json
import mysql.connector as mysql
import pandas as pd


# Conexión al sql
db = mysql.connect(
    host="localhost",
    user="root",
    password="mysqlcomputacion2025",
    database = "ProyectoIntegrador"
)
cursor = db.cursor(dictionary=True)

def limpiar_cadena(valor):
    return valor.strip().replace("'",'')

def limpiar_float(valor):
    try:
        return float(valor) if valor != "" else 0.0
    except ValueError:
        return 0.0

def limpiar_int(valor):
    try:
        return int(valor)
    except ValueError:
        return 0

def limpiar_date(valor):
    if valor == '':
        return '3000-01-01'
    else:
        return valor

def transform_to_json(valor):
    valor = valor.replace("''", " ")
    valor = valor.replace(" '", " ")
    valor = valor.replace("'", '"')
    try:
        data = json.loads(valor)
    except:
        data = []
    return data

def limpiar_cadenaGenre(valor):
    return valor.strip().replace(" ", ',')

def procesar(movie):
    # Metadatos
    df = pd.read_csv('C:/Users/Usuario/Desktop/movie_dataset.csv')
    max_value = max(map(lambda x: x, df['budget']))  # Presupuesto mayor
    print(max_value)
    min_value = min(map(lambda x: x, df['budget']))  # Presupuesto menor
    print(min_value)
    # Exploración de datos
    print(df.head())
    # Lectura csv
    with open('C:/Users/Usuario/Desktop/movie_dataset.csv', newline='', encoding="utf8") as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',')
        next(spamreader)
        # Limpieza de datos
        for row in spamreader:
            budget = limpiar_int(row[1])  # int
            genres = limpiar_cadenaGenre(row[2])  # str
            homepage = limpiar_cadena(row[3])  # str
            id_movie = limpiar_int(row[4])  # int
            keywords = limpiar_cadena(row[5])  # str
            original_language = limpiar_cadena(row[6])  # str
            original_title = limpiar_cadena(row[7])  # str
            overview = limpiar_cadena(row[8])  # str
            popularity = limpiar_float(row[9]) # float
            production_companies = transform_to_json(row[10])  # json
            production_countries = transform_to_json(row[11])  # json
            release_date = limpiar_date(row[12])  # date
            revenue = limpiar_int(row[13])  # long
            runtime = limpiar_int(row[14])  # int
            spoken_languages = transform_to_json(row[15])  # json
            status = limpiar_cadena(row[16])  # str
            tagline = limpiar_cadena(row[17])  # str
            title = limpiar_cadena(row[18])  # str
            vote_average = limpiar_float(row[9])# float
            vote_count = limpiar_int(row[20])  # int
            cast = limpiar_cadena(row[21])  # str
            crew = transform_to_json(row[22])  # json
            director = limpiar_cadena(row[23])  # str
            # Población de datos
            print(row)
            query_movie = f"INSERT INTO Movie (id, budget, genres, homepage, original_language, original_title, " \
                          f"overview, popularity, release_date, revenue, runtime, status, tagline, title, vote_average, " \
                          f"vote_count, director) VALUES ('{id_movie}', '{budget}', '{genres}', '{homepage}', " \
                          f"'{original_language}', '{original_title}', '{overview}', '{popularity}', '{release_date}', " \
                          f"'{revenue}', '{runtime}', '{status}', '{tagline}', '{title}', '{vote_average}', '{vote_count}', " \
                          f"'{director}');"
            print(query_movie)
            cursor.execute(query_movie)
            db.commit()

            for obj in keywords:
                print(obj)
                query_keywords = f"INSERT INTO Keywords (idMovie, Name) VALUES ({id_movie}, '{obj['Name']}');"
                print(query_keywords)
                cursor.execute(query_keywords)
                db.commit()

            for obj in production_companies:
                print(obj)
                query_production_companies = f"INSERT INTO production_companies (idMovie, name, idCompany) VALUES ({id_movie}, " \
                                         f"'{obj['name']}', '{obj['idCompany']}');"
                print(query_production_companies)
                cursor.execute(query_production_companies)
                db.commit()

            for obj in crew:
                print(obj)
                query_crew = f"INSERT INTO Crew (idMovie, name, gender, department, job, credit_id, id) VALUES ({id_movie}, " \
                             f"'{obj['name']}', '{obj['gender']}', '{obj['department']}', '{obj['job']}', '{obj['credit_id']}', " \
                             f"'{obj['id']}');"
                print(query_crew)
                cursor.execute(query_crew)
                db.commit()

            for obj in spoken_languages:
                print(obj)
                query_spoken_languages = f"INSERT INTO spoken_language (idMovie, iso_639_1, name) VALUES ({id_movie}, " \
                                         f"'{obj['iso_639_1']}', '{obj['name']}');"
                print(query_spoken_languages)
                cursor.execute(query_spoken_languages)
                db.commit()

            for obj in production_countries:
                print(obj)
                query_production_countries = f"INSERT INTO production_countries (idMovie, iso_3166_1, name) VALUES ({id_movie}, " \
                                             f"'{obj['iso_3166_1']}', '{obj['name']}');"
                print(query_production_countries)
                cursor.execute(query_production_countries)
                db.commit()

            for obj in cast:
                print(obj)
                query_spoken_languages = f"INSERT INTO Cast (idMovie, Name) VALUES ({id_movie}, " \
                                         f"'{obj['Name']}');"
                print(query_spoken_languages)
                cursor.execute(query_spoken_languages)
                db.commit()


def push_rows(observer, scheduler):
    with open('C:/Users/Usuario/Desktop/movie_dataset.csv', newline='', encoding = "utf8") as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',')
        next(spamreader)
        for row in spamreader:
            observer.on_next(row)
        observer.on_completed()

source = create(push_rows)

# SUBSCRIBE A CADA EVENTO QUE EMITA EL OBSERVABLE
source.subscribe(
    on_next=lambda movie: procesar(movie),
    on_error=lambda e: print("Error Occurred: {0}".format(e)),
    on_completed=lambda: print("Done!"),
)