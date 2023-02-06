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
  /*val Movie_production_companies = data
      .map(x =>
        (x("id"), Json.parse(x("production_companies")), Json.parse(x("production_companies"))))
      .map(x => (x._1, x._2 \\ "id", x._3 \\ "name"))
      .filter(x => x._2.nonEmpty && x._3.nonEmpty)
      .flatMap(x => x._2.zip(x._3).map((x._1, _)))
      .map(x => (x._1, x._2._1.as[Int], x._2._2.as[String]))

  Movie_production_companies.map(elem =>
    sql"""
         |INSERT INTO production_companies (idMovie, name, idCompany)
         |VALUES
         |(${elem._1}, ${elem._3}, ${elem._2})
                     """.stripMargin
      .update
      .apply())*/

   //COLUMNA production_countries
   /*val Movie_production_countries = data
     .map(x =>
       (x("id"), Json.parse(x("production_countries")), Json.parse(x("production_countries"))))
     .map(x => (x._1, x._2 \\ "iso_3166_1", x._3 \\ "name"))
     .filter(x => x._2.nonEmpty && x._3.nonEmpty)
     .flatMap(x => x._2.zip(x._3).map((x._1, _)))
     .map(x => (x._1, x._2._1.as[String], x._2._2.as[String]))

  Movie_production_countries.map(elem =>
    sql"""
         |INSERT INTO production_countries (idMovie, name, `iso_3166_1`)
         |VALUES
         |(${elem._1}, ${elem._3}, ${elem._2})
                         """.stripMargin
      .update
      .apply())*/

  //COLUMNA spoken_language
  /*val spoken_language = data
    .map(x =>
      (x("id"), Json.parse(x("spoken_languages")), Json.parse(x("spoken_languages"))))
    .map(x => (x._1, x._2 \\ "iso_639_1", x._3 \\ "name"))
    .filter(x => x._2.nonEmpty && x._3.nonEmpty)
    .flatMap(x => x._2.zip(x._3).map((x._1, _)))
    .map(x => (x._1, x._2._1.as[String], x._2._2.as[String]))

  spoken_language.map(elem =>
    sql"""
         |INSERT INTO spoken_language (idMovie, iso_639_1, `name`)
         |VALUES
         |(${elem._1}, ${elem._2}, ${elem._3})
                           """.stripMargin
      .update
      .apply())*/

  //COLUMNA Crew
  /*val pattern1 = "(\\s\"(.*?)\",)".r
  val pattern2 = "([a-z]\\s\"(.*?)\"\\s*[A-Z])".r
  val pattern3 = "(:\\s'\"(.*?)',)".r

  def replacePattern4(original: String, pattern: Regex): String = {
    var txtOr = original
    for (m <- pattern.findAllIn(original)) {
      if (pattern == pattern2) {
        txtOr = txtOr.replace(m, m.replace("\"", "-u0022"))
      } else if (pattern == pattern1) {
        txtOr = txtOr.replace(m, m.replace("'", "-u0027"))
      } else if (pattern == pattern3) {
        txtOr = txtOr.replace(m, m.replace("\"", "-u0022"))
      }
    }
    txtOr
  }

  val crew = data
    .map(row => row("crew"))
    .map(text => replacePattern4(text, pattern2))
    .map(text => replacePattern4(text, pattern1))
    .map(text => replacePattern4(text, pattern3))
    .map(text => text.replace("'", "\""))
    .map(text => text.replace("-u0027", "'"))
    .map(text => text.replace("-u0022", "\\\""))


  val id = data
    .map(row => row("id"))

  val crewId = id.zip(crew)
    .map(x =>
      (x._1, Json.parse(x._2), Json.parse(x._2), Json.parse(x._2), Json.parse(x._2), Json.parse(x._2), Json.parse(x._2)))
    .map(x => (x._1, x._2 \\ "name", x._3 \\ "gender", x._4 \\ "department", x._5 \\ "job", x._6 \\ "credit_id", x._7 \\ "id"))
    .filter(x => x._2.nonEmpty && x._3.nonEmpty)
    .flatMap(x => x._2.zip(x._3).zip(x._4).zip(x._5).zip(x._6).zip(x._7).map((x._1, _)))
    .map{ case (id, (((((name, gender), department), job), creditId), personId)) =>
      (id, name.as[String], gender.as[Int], department.as[String], job.as[String], creditId.as[String], personId.as[Int])
    }

  crewId.map(elem =>
    sql"""
         |INSERT INTO Crew (idMovie, id, name, gender, departament, job, credit_id)
         |VALUES
         |(${elem._1}, ${elem._7}, ${elem._2}, ${elem._3}, ${elem._4}, ${elem._5}, ${elem._6})
                     """.stripMargin
      .update
      .apply())
*/
}
