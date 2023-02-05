package ProyectoIntegrador

import java.io.File
import com.github.tototoshi.csv.CSVReader
import io.circe.JsonObject
import scalikejdbc._
import play.api.libs.json._
import requests.Response

import scala.util.{Success, Try}
import scala.util.matching.Regex

object SentenciasInsertInto extends App {
  val reader = CSVReader.open(new File("C:\\Users\\CM\\movie_dataset.csv"))
  val data = reader.allWithHeaders()
  reader.close()

  Class.forName("com.mysql.cj.jdbc.Driver")
  ConnectionPool.singleton("jdbc:mysql://localhost:3306/ProyectoIntegrador", "root", "nomeacuerdo")
  implicit val session: DBSession = AutoSession

  //COLUMNA GENRES - Pasado A Base De Datos
  /*val generos = data
    .map(elem => (elem("id"), elem("genres").replace("Science Fiction", "Science_Fiction")))
    .filter(_._2.nonEmpty)
    .map(x => (x._1, x._2.split(" ")))
    .flatMap(x => x._2.map((x._1, _)))
    .map(x => (x._1.toInt, x._2))
    .sortBy(_._1)

    generos.map(elem =>
    sql"""
         |INSERT INTO genres(idMovie, name)
         |VALUES
         |(${elem._1}, ${elem._2})
               """.stripMargin
      .update
      .apply())*/

  //COLUMNA CAST - Pasado A Base De Datos
  /*val cast = data
    .map(elem => (elem("id"), elem("cast")))
    .filter(_._2.nonEmpty)
    .map(x => (x._1, x._2.split(" ")))
    .flatMap(x => x._2.map((x._1, _)))
    .map(x => (x._1.toInt, x._2))
    .sortBy(_._1)

  cast.map(elem =>
    sql"""
         |INSERT INTO Cast (idMovie, Name)
         |VALUES
         |(${elem._1}, ${elem._2})
                 """.stripMargin
      .update
      .apply())*/

  //COLUMNA KEYWORDS
  /*val claves = data
    .map(elem => (elem("id"), elem("keywords")))
    .filter(_._2.nonEmpty)
    .map(x => (x._1, x._2.split(" ")))
    .flatMap(x => x._2.map((x._1, _)))
    .map(x => (x._1.toInt, x._2))
    .sortBy(_._1)

  claves.map(elem =>
    sql"""
         |INSERT INTO Keywords (idMovie, Name)
         |VALUES
         |(${elem._1}, ${elem._2})
                   """.stripMargin
      .update
      .apply())*/

  //COLUMNA production_companies
  /*val productionCompanies = data
    .map(elem => (elem("id"), elem("production_companies")))

  println(productionCompanies)

  productionCompanies.map(elem =>
    sql"""
         |INSERT INTO production_companies (idMovie, name, idCompany)
         |VALUES
         |(${elem._1}, ${Json.parse(elem._2) \\ "name"}, ${Json.parse(elem._2) \\ "id"})
                     """.stripMargin
      .update
      .apply())*/

   //COLUMNA production_countries
  /*val productionCountries = data
    .map(elem => (elem("id"), elem("production_countries")))

  println(productionCountries)

  productionCountries.map(elem =>
    sql"""
         |INSERT INTO production_countries (idMovie, name, `iso_3166_1`)
         |VALUES
         |(${elem._1}, ${Json.parse(elem._2) \\ "name"}, ${Json.parse(elem._2) \\ "iso_3166_1"})
                         """.stripMargin
      .update
      .apply())*/

  //COLUMNA spoken_language
  /*val spoken_language = data
    .map(elem => (elem("id"), elem("spoken_languages")))

  println(spoken_language)

  spoken_language.map(elem =>
    sql"""
         |INSERT INTO spoken_language (idMovie, `iso_639_1`, name)
         |VALUES
         |(${elem._1}, ${Json.parse(elem._2) \\ "iso_639_1"}, ${Json.parse(elem._2) \\ "name"})
                     """.stripMargin
      .update
      .apply())*/

  //COLUMNA Crew
  val pattern1 = "(\\s\"(.*?)\",)".r
  val pattern2 = "([a-z]\\s\"(.*?)\"\\s*[A-Z])".r
  val pattern3 = "(:\\s'\"(.*?)',)".r

  def replacePattern4(original: String, pattern: Regex): String = {
    var txtOr = original

    for (m <- pattern.findAllIn(original)) {
      val textOriginal = m
      var replaceText = m
      if (pattern == pattern2) {
        replaceText = m.replace("\"", "-u0022")
      } else if (pattern == pattern1) {
        replaceText = m.replace("'", "-u0027")
      } else if (pattern == pattern3) {
        replaceText = m.replace("\"", "-u0022")
      }
      txtOr = txtOr.replace(textOriginal, replaceText)
    }
    txtOr
  }

  val crew = data
    .map(elem => (elem("id") ->
      elem("crew")))
    .map(_._2)
    .map(text => replacePattern4(text, pattern2))
    .map(text => replacePattern4(text, pattern1))
    .map(text => replacePattern4(text, pattern3))
    .map(text => text.replace("'", "\""))
    .map(text => text.replace("-u0027", "'"))
    .map(text => text.replace("-u0022", "\\\""))
    .map(text => Try(Json.parse(text)) match {
      case Success(i) => i
    })
    .flatMap(_.as[List[JsValue]])
    .map(x => (x("name").as[String],
      x("gender").as[Int],
      x("department").as[String],
      x("job").as[String],
      x("credit_id").as[String],
      x("id").as[Int]))

  print(crew)
  /*idMovieCrew.map(elem =>
    sql"""
         |INSERT INTO Crew (idMovie, id, name, gender, departament, job, credit_id)
         |VALUES
         |(${elem._1}, ${Json.parse(elem._2) \\ "id"}, ${Json.parse(elem._2) \\ "name"}, ${Json.parse(elem._2) \\ "gender"}, ${Json.parse(elem._2) \\ "departament"}, ${Json.parse(elem._2) \\ "job"}, ${Json.parse(elem._2) \\ "credit_id"})
                     """.stripMargin
      .update
      .apply())*/

}
