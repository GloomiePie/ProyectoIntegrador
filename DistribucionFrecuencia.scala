package ProyectoIntegrador

import breeze.numerics.cbrt
import com.github.tototoshi.csv._

import java.io.File
import com.cibo.evilplot.plot._
import com.cibo.evilplot.plot.aesthetics.DefaultTheme._

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object DistribucionFrecuencia extends App{
  val reader = CSVReader.open(new File("C:\\Users\\CM\\movie_dataset.csv"))
  val data = reader.allWithHeaders()
  reader.close()

  //////////////////////////////////
  println("Columna genres")
  val generos = data
    .flatMap(elem => elem.get("genres"))

  generos.map(x => x.replace(" ", ","))
  val generoMax = generos.groupBy(x => x).toList.maxBy(_._2.size)._2.size
  val nomGeneroMax = generos.groupBy(x => x).toList.maxBy(_._2.size)._1
  val generoMin = generos.groupBy(x => x).toList.minBy(_._2.size)._2.size
  val nomGeneroMin = generos.groupBy(x => x).toList.minBy(_._2.size)._1
  val sinGenero = generos.groupBy(x => x).toList.filter(_._1.isEmpty).maxBy(_._2.size)._2.size
  println("El genero que más se repite es " + nomGeneroMax + " y se repite " + generoMax + " veces")
  println("El genero que menos se repite es " + nomGeneroMin + " y se repite " + generoMin + " veces")
  println("Existen en total " + sinGenero + " peliculas sin genero")

  val generos1: Seq[Double] = Seq(cbrt(generoMax), cbrt(generoMin), cbrt(sinGenero), 0)
  val generos2 = List(nomGeneroMax, "Adventure", "SinGenero")
  BarChart(generos1)
    .title("Datos descriptivos")
    .xAxis(generos2)
    .yAxis()
    .frame()
    .yLabel("Genero")
    .bottomLegend()
    .render()
    .write(new File("C:\\Users\\CM\\Desktop\\genero.png"))

  //////////////////////////////////
  println("\nColumna original_language")
  val original = data
    .flatMap(elem => elem.get("original_language"))

  val languagesMax = original.groupBy(x => x).toList.maxBy(_._2.size)._2.size
  val nomlanguagesMax = original.groupBy(x => x).toList.maxBy(_._2.size)._1
  val languagesMin = original.groupBy(x => x).toList.minBy(_._2.size)._2.size
  val nomlanguagesMin = original.groupBy(x => x).toList.minBy(_._2.size)._1
  println("El Lenguaje Original que más se repite es " + nomlanguagesMax.toUpperCase() + " y se repite " + languagesMax + " veces")
  println("El Lenguaje Original que menos se repite es " + nomlanguagesMin.toUpperCase() + " y se repite " + languagesMin + " veces")

  val language1: Seq[Double] = Seq(cbrt(languagesMax), cbrt(languagesMin), 0)
  val language2 = List(nomlanguagesMax, nomlanguagesMin)
  BarChart(language1)
    .title("Datos descriptivos")
    .xAxis(language2)
    .yAxis()
    .frame()
    .yLabel("LENGUAJES ORIGINALES")
    .bottomLegend()
    .render()
    .write(new File("C:\\Users\\CM\\Desktop\\lenguages.png"))

  //////////////////////////////////
  println("\nColumna status")

  val estado = data
    .flatMap(elem => elem.get("status"))

  val statusMax = estado.groupBy(x => x).toList.maxBy(_._2.size)._2.size
  val nomstatusMax = estado.groupBy(x => x).toList.maxBy(_._2.size)._1
  val statusMin = estado.groupBy(x => x).toList.minBy(_._2.size)._2.size
  val nomstatusMin = estado.groupBy(x => x).toList.minBy(_._2.size)._1
  val statusRumored = estado.groupBy(x => x).toList.filter(_._1 == "Rumored").maxBy(_._2.size)._2.size
  println("Hay " + statusMax + " peliculas en estado " + nomstatusMax)
  println("Hay " + statusMin + " peliculas en estado " + nomstatusMin)
  println("Hay " + statusRumored + " peliculas en estado Rumored")

  val status1: Seq[Double] = Seq(cbrt(statusMax), cbrt(statusMin), cbrt(statusRumored), 0)
  val status2 = List(nomstatusMax, nomstatusMin, "Rumored")

  BarChart(status1)
    .title("Datos descriptivos")
    .xAxis(status2)
    .yAxis()
    .frame()
    .yLabel("Status")
    .bottomLegend()
    .render()
    .write(new File("C:\\Users\\CM\\Desktop\\status.png"))

  //////////////////////////////////
  println("\nColumna Overview")
  val overview = data
    .flatMap(elem => elem.get("overview"))

  val sinOverview = overview.groupBy(x => x).toList.filter(_._1.isEmpty).maxBy(_._2.size)._2.size
  println("Existen en total " + sinOverview + " peliculas que no tienen Overview")

  //////////////////////////////////
  println("\nColumna Homepage")
  val homepage = data
    .flatMap(elem => elem.get("homepage"))

  val sinHomepage = homepage.groupBy(x => x).toList.filter(_._1.isEmpty).maxBy(_._2.size)._2.size
  println("Existen un total de " + sinHomepage + " peliculas que no tienen registrados un Homepage")

  //////////////////////////////////
  println("\nColumna Keywords")
  val keywords = data
    .flatMap(elem => elem.get("keywords"))

  val sinKeywords = keywords.groupBy(x => x).toList.filter(_._1.isEmpty).maxBy(_._2.size)._2.size
  println(sinKeywords + " peliculas que no poseen Keywords")

  //////////////////////////////////
  println("\nColumna Tagline")
  val tagline = data
    .flatMap(elem => elem.get("tagline"))
  val sinTagline = tagline.groupBy(x => x).toList.filter(_._1.isEmpty).maxBy(_._2.size)._2.size
  println(sinTagline + " peliculas no han registrado su Tagline")

  //////////////////////////////////
  println("\nColumna Release_Date")
  val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val releaseDate = data
    .map(elem => elem("release_date"))
    .filter(!_.equals(""))
    .map(text => LocalDate.parse(text, dateFormatter))

  val yearReleaseDate = releaseDate
    .map(_.getYear)
    .map(_.toDouble)

  printf("La pelicula más antigua registrada es del año %.0f\n", yearReleaseDate.min)
  printf("La pelicula mas reciente registrada es del año %.0f\n", yearReleaseDate.max)

  Histogram(yearReleaseDate)
    .title("Años de Lanzamiento")
    .xAxis()
    .yAxis()
    .xbounds(1916.0, 2018.0)
    .render()
    .write(new File("C:\\Users\\CM\\Desktop\\añosLanzamiento.png"))
}