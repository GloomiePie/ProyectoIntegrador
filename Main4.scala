package ProyectoIntegrador

import com.github.tototoshi.csv.CSVReader

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}

object Main4 extends App{
  val reader = CSVReader.open(new File("C:\\Users\\SALA A\\Downloads\\movie_dataset.csv"))
  val data = reader.allWithHeaders()
  reader.close()

  def escapeMysql(text: String): String = text
    .replaceAll("\\\\", "\\\\\\\\")
    .replaceAll("\b", "\\\\b")
    .replaceAll("\n", "\\\\n")
    .replaceAll("\r", "\\\\r")
    .replaceAll("\t", "\\\\t")
    .replaceAll("\\x1A", "\\\\Z")
    .replaceAll("\\x00", "\\\\0")
    .replaceAll("'", "\\\\'")
    .replaceAll("\"", "\\\\\"")

  case class Movie(
                    id: Int,
                    index: Int,
                    original_language: String,
                    original_title: String,
                    overview: String,
                    budget: Long,
                    popularity: Double,
                    runtime: Double,
                    revenue: Long
                  )

  val movieData = data
    .map(row => Movie(
      row("id").toInt,
      row("index").toInt,
      row("original_language"),
      row("original_title"),
      row("overview") match {
        case valueOfOver if valueOfOver.trim.isEmpty => "No registrado"
        case valueOfOver => valueOfOver
      },
      row("budget").toLong,
      row("popularity").toDouble,
      row("runtime") match {
        case valueOfRun if valueOfRun.trim.isEmpty => 0.0
        case valueOfRun => valueOfRun.toDouble
      },
      row("revenue").toLong))

  val SQL_INSERT_PATTERN =
    """INSERT INTO MOVIE ('id', 'index ', 'original_language', 'original_title', 'overview', 'budget', 'popularity', 'runtime', 'revenue')
      |VALUES
      |('%s', %d ,'%s', '%s', '%s', '%d', '%f', '%f', '%d')
      |""".stripMargin

  val scriptData = movieData
    .map(movie => SQL_INSERT_PATTERN.formatLocal(java.util.Locale.US,
      movie.id,
      movie.index,
      escapeMysql(movie.original_language),
      escapeMysql(movie.original_title),
      escapeMysql(movie.overview),
      movie.budget,
      movie.popularity,
      movie.runtime,
      movie.revenue
    ))

  val ScriptFile = new File("C:\\Users\\CM\\Desktop\\movies_insert.sql")
  if(ScriptFile.exists()) ScriptFile.delete()

  scriptData.foreach(insert =>
    Files.write(Paths.get("C:\\Users\\SALA A\\Desktop\\movies_insert.sql"), insert.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  )
}