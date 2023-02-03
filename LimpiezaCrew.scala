package ProyectoIntegrador
import com.github.tototoshi.csv.CSVReader
import play.api.libs.json._

import java.io.File
import scala.util.{Failure, Success, Try}
import scala.util.matching.Regex

object LimpiezaCrew extends App {
  val reader = CSVReader.open(new File("C:\\Users\\CM\\movie_dataset.csv"))
  val data = reader.allWithHeaders()
  reader.close()

  val pattern1 = "(\\s\"(.*?)\",)".r
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
    .distinct

  println(crew)

}
