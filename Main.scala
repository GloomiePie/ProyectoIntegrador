package ProyectoIntegrador

import com.github.tototoshi.csv._
import java.io.File

import play.api.libs.json._

object Main extends App {
  val reader = CSVReader.open(new File("C:\\Users\\CM\\movie_dataset.csv"))
  val data = reader.allWithHeaders()
  reader.close()

  val lenguajes = data
    .flatMap(elem => elem.get("spoken_languages" ))

  val json: JsValue = Json.parse(
    """
      [{"iso_639_1": "en", "name": "English"},
      {"iso_639_1": "fr", "name": "Fran\u00e7ais"},
      {"iso_639_1": "de", "name": "Deutsch"}]
      [{"iso_639_1": "en", "name": "English"},
      {"iso_639_1": "es", "name": "Espa\u00f1ol"}]
       """)

  println(Json.stringify(json))

  val lat = json \\ "iso_639_1"
  println(lat)
  val names = json \\ "name"
  println(names)
}
