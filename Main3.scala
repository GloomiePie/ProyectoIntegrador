package ProyectoIntegrador

import com.github.tototoshi.csv._
import java.io.File

import com.cibo.evilplot.plot._
import com.cibo.evilplot.plot.aesthetics.DefaultTheme._

object Main3 extends App{
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

  val generos1: Seq[Double] = Seq(generoMax, generoMin, sinGenero)
  val generos2 = List(nomGeneroMax, nomGeneroMin, "SinGenero")

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

  val language1: Seq[Double] = Seq(languagesMax, languagesMin)
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

  val status1: Seq[Double] = Seq(statusMax, statusMin, statusRumored)
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
}