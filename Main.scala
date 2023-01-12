package ProyectoIntegrador

import com.github.tototoshi.csv._
import java.io.File

import play.api.libs.json._

object Main extends App {

  val reader = CSVReader.open(new File("C:\\Users\\SALA A\\Downloads\\movie_dataset.csv"))
  val data = reader.allWithHeaders()
  reader.close()

  //Ejemplo Basico Json
  val json: JsValue = Json.parse(
    """
      [
      {
        "iso_639_1": "en",
        "name": "English"
      },
      {
        "iso_639_1": "fr",
        "name": "Fran\u00e7ais"
      },
      {
        "iso_639_1": "de",
        "name": "Deutsch"
      }
    ][
      {
        "iso_639_1": "en",
        "name": "English"
      },
      {
        "iso_639_1": "es",
        "name": "Espa\u00f1ol"
      }
       """)

  println("Pelicula => AVENGERS INFINITY WARS")

  val lat = json \\ "iso_639_1"
  println("Esta Pelicula Esta Doblada En: " + lat.map(x => x).toList.size + " Idiomas")
  val names = json \\ "name"
  println("Dichos idiomas son: ")
  names.map(x => x).toList.foreach(arg => println(arg))
  println("")

  //Ejemplo Usando Columnas/DATASET - Json

  //JSON - COLUMNA PRODUCTION_COMPANIES
  val productionCompanies = data
    .flatMap(row => row.get("production_companies"))
    .map(row => Json.parse(row))
    .flatMap(jsonData => jsonData \\ "name")
    .map(jsValue => jsValue.as[String])
    .groupBy(identity)
    .map { case (keyword, lista) => (keyword, lista.size) }
    .toList
    .sortBy(_._2)
    .reverse

  println("El top 3 de Compañias que mas se repiten son: ")
  productionCompanies.take(3).map(x => x._1)
    .foreach(arg => println(arg))

  println("")

  //JSON - COLUMNA PRODUCTION_COUNTRIES
  val productionCountries = data
    .flatMap(row => row.get("production_countries"))
    .map(row => Json.parse(row))
    .flatMap(jsonData => jsonData \\ "name")
    .map(jsValue => jsValue.as[String])
    .groupBy(identity)
    .map { case (keyword, lista) => (keyword, lista.size) }
    .toList
    .sortBy(_._2)
    .reverse

  println("El top 5 de los Paises de las Producciones que mas se repiten son: ")
  productionCountries.take(5).map(x => x._1)
    .foreach(arg => println(arg))

  println("")

  //JSON - COLUMNA SPOKEN_LANGUAGES
  val spokenLanguages = data
    .flatMap(row => row.get("spoken_languages"))
    .map(row => Json.parse(row))
    .flatMap(jsonData => jsonData \\ "name")
    .map(jsValue => jsValue.as[String])
    .groupBy(identity)
    .map { case (keyword, lista) => (keyword, lista.size) }
    .toList
    .sortBy(_._2)
    .reverse

    println("La Gran Mayoria de peliculas se han traducido a varios Idiomas de los 10 Idiomas mas traducidos son:")
    spokenLanguages.take(10).map(x => x._1)
      .foreach(arg => print(arg + " - "))

    println("")
    print("En el que el TOP 1 se ha traducido: ")
    spokenLanguages.take(1).map(x => x._2)
      .foreach(arg => println(arg))
}