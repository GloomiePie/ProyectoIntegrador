import com.github.tototoshi.csv._
import java.io.File

object Main3 extends App{
  val reader = CSVReader.open(new File("C:\\Users\\USUARIO\\Downloads\\movie_dataset.csv"))
  val data = reader.allWithHeaders()
  reader.close()

  println("Columna genres")
  val generos = data
    .flatMap(elem => elem.get("genres"))

  printf("la columna genres tiene %s filas con carácteres\n\n", generos.filter(x => x != " ")
    .count(x => x != ""))

  println("Columna homepage")
  val url = data
    .flatMap(elem => elem.get("homepage"))

  printf("la columna homepage tiene %s filas con carácteres\n\n", url.filter(x => x != " ")
    .count(x => x != ""))

  println("Columna keywords")
  val clave = data
    .flatMap(elem => elem.get("keywords"))

  printf("la columna keywords tiene %s filas con carácteres\n\n", clave.filter(x => x != " ")
    .count(x => x != ""))

  println("Columna original_language")
  val original = data
    .flatMap(elem => elem.get("original_language"))

  printf("la columna original_languajes tiene %s filas " +
    "con el carácter 'en'\n", original.filter(x => x == "en")
    .count(x => x != ""))

  printf("la columna original_languajes tiene %s filas " +
    "con el carácter 'es'\n", original.filter(x => x == "es")
    .count(x => x != ""))

  printf("la columna original_languajes tiene %s filas " +
    "con el carácter 'fr'\n", original.filter(x => x == "fr")
    .count(x => x != ""))

  printf("la columna original_languajes tiene %s filas " +
    "con el carácter 'ja'\n", original.filter(x => x == "ja")
    .count(x => x != ""))

  printf("la columna original_languajes tiene %s filas " +
    "con el carácter 'ru'\n", original.filter(x => x == "ru")
    .count(x => x != ""))

  printf("la columna original_languajes tiene %s filas " +
    "con el carácter 'ko'\n\n", original.filter(x => x == "ko")
    .count(x => x != ""))

  println("Columna status")
  val estado = data
    .flatMap(elem => elem.get("status"))

  printf("la columna status tiene %s filas " +
    "con el carácter 'Released'\n", estado.filter(x => x == "Released")
    .count(x => x != ""))

  printf("la columna status tiene %s filas " +
    "con el carácter 'Post Production'\n", estado.filter(x => x == "Post Production")
    .count(x => x != ""))

  printf("la columna status tiene %s filas " +
    "con el carácter 'Rumored'\n", estado.filter(x => x == "Rumored")
    .count(x => x != ""))
}
