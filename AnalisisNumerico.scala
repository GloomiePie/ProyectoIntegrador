import com.cibo.evilplot.plot._
import com.cibo.evilplot.plot.aesthetics.DefaultTheme._
import com.github.tototoshi.csv.CSVReader

import java.io.File

object AnalisisNumerico extends App{
  val reader = CSVReader.open(new File("C:\\Users\\USUARIO\\Downloads\\movie_dataset.csv"))
  val data = reader.allWithHeaders()
  reader.close()

  //Columna budget
  println("Columna budget")
  val presupuesto = data
    .flatMap(elem => elem.get("budget"))

  printf("El presupuesto más alto es: $%s\n", presupuesto.maxBy(x => x.toInt)) //Conseguir el mayor presupuesto
  printf("El presupuesto más bajo es: $%s\n", presupuesto.minBy(x => x.toInt)) //Conseguir el menor presupuesto
  printf("El presupuesto más bajo sin 0 es: $%s\n", presupuesto.filter(x => x != "0").minBy(x => x.toInt)) //Conseguir el menor presupuesto sin 0
  printf("El presupuesto promedio es: $%.2f\n", presupuesto.map(x => x.toDouble).sum
    / presupuesto.size) //Conseguir el promedio

  val budget: Seq[Double] = Seq(presupuesto.count(_ == "0").toDouble/1000,//cuantos valores nulos hay
    presupuesto.count(x => x.toDouble < 10000).toDouble/1000,//cuantos presupuestos estan por debajo de los 1000 dolares
    presupuesto.count(x => x.toDouble > 10000000).toDouble/1000, 0)//cuantos presupuestos estan por encima del 1000000

  val medidas = List("Cantidad nulos", "Inferiores a 1000$", "Superiores a 1000000$")

  BarChart(budget)
    .title("Datos descriptivos")
    .xAxis(medidas)
    .yAxis()
    .frame()
    .yLabel("BUDGET")
    .bottomLegend()
    .render()
    .write(new File("C:\\Users\\USUARIO\\Downloads\\budget.png"))
  println("Gráfico generado\n")

  // Columna popularity
  println("Columna popularity")
  val popularidad = data
    .flatMap(elem => elem.get("popularity"))

  printf("La popularidad más alto es: %s\n", popularidad.maxBy(x => x.toDouble)) //Conseguir la mayor popularidad
  printf("La popularidad más bajo es: %s\n", popularidad.minBy(x => x.toDouble)) //Conseguir el menor popularidad
  printf("La popularidad más bajo sin 0 es: %s\n", popularidad.filter(x => !x.equals("0.0")).minBy(x => x.toDouble)) //Conseguir el menor popularidad sin 0
  printf("La popularidad promedio es: %.2f\n", popularidad.map(x => x.toDouble).sum
    / popularidad.size) //Conseguir el promedio

  val popularity : Seq[(String,Double)] = Seq(("Valores mayores a 500" ->
    popularidad.count(_ > "500")), //cuantos valores mayores a 500 hay
    ("Valores mayores al promedio"->
      popularidad.count(x => x.toDouble >= (x.map(_.toDouble).sum/x.size)).toDouble) //cauntos valores superan el promedio
  )

  PieChart(popularity)
    .title("Datos descriptivos")
    .rightLegend()
    .render()
    .write(new File("C:\\Users\\USUARIO\\Downloads\\popularity.png"))
  println("Gráfico generado\n")

  // Columna revenue
  println("Columna revenue")
  val ganancias = data
    .flatMap(elem => elem.get("revenue"))

  printf("La ganancia más alto es: $%s\n", ganancias.maxBy(x => x.toDouble)) //Conseguir la mayor ganacia
  printf("La ganancia más bajo es: $%s\n", ganancias.minBy(x => x.toDouble)) //Conseguir la menor ganacia
  printf("La ganancia más bajo sin 0 es: $%s\n", ganancias.filter(x => !x.equals("0")).minBy(x => x.toDouble)) //Conseguir la menor ganacia sin 0
  printf("La ganancia promedio es: $%.2f\n", ganancias.map(x => x.toDouble).sum
    / ganancias.size) //Conseguir el promedio

  println(ganancias.count(x => x.toDouble >= (x.map(_.toDouble).sum/x.size)).toDouble/1000)
  println( presupuesto.count(_ == "0").toDouble/1000)

  val revenue: Seq[Double] = Seq(ganancias.count(x => x.toDouble >= (x.map(_.toDouble).sum/x.size)).toDouble/1000,
    presupuesto.count(_ == "0").toDouble/1000, 0)

  val revenue1 = List("Valores mayor al promedio", "Valores igual a 0")
  BarChart(revenue)
    .title("Datos descriptivos")
    .xAxis(revenue1)
    .yAxis()
    .frame()
    .yLabel("REVENUE")
    .bottomLegend()
    .render()
    .write(new File("C:\\Users\\USUARIO\\Downloads\\revenue.png"))
  println("Gráfico generado\n")

  // Columna vote average
  println("Columna vote average")
  val promedio = data
    .flatMap(elem => elem.get("vote_average"))

  printf("El promedio más alto es: %s\n", promedio.maxBy(x => x.toDouble)) //Conseguir la mayor ganacia
  printf("El promedio más bajo es: %s\n", promedio.minBy(x => x.toDouble)) //Conseguir la menor ganacia
  printf("El promedio más bajo sin 0 es: %s\n", promedio.filter(x => !x.equals("0.0")).minBy(x => x.toDouble)) //Conseguir la menor ganacia sin 0
  printf("El promedio es: %.2f\n", promedio.map(x => x.toDouble).sum
    / promedio.size) //Conseguir el promedio

  val vote_average: Seq[(String, Double)] = Seq(("valores mayor a 8.0" -> promedio.count(x => x >= "8.0").toDouble), //valores mayor a 8.0
    ("valores menores a 4.7" -> promedio.count(x => x <= "4.7" ).toDouble),//valores menores a 4.7
   ("valores igual a 5.0" -> promedio.count(_.equals("5.0")).toDouble), //valores igual a 5.0
  )

  PieChart(vote_average)
    .title("Datos descriptivos")
    .rightLegend()
    .render()
    .write(new File("C:\\Users\\USUARIO\\Downloads\\vote_average.png"))
  println("Gráfico generado\n")

  // Columna vote count
  println("Columna vote average")
  val votos = data
    .flatMap(elem => elem.get("vote_count"))

  printf("El voto más alto es: %s\n", votos.maxBy(x => x.toInt)) //Conseguir la mayor ganacia
  printf("El voto más bajo es: %s\n", votos.minBy(x => x.toInt)) //Conseguir la menor ganacia
  printf("El voto promedio es: %.2f\n", votos.map(x => x.toDouble).sum
    / votos.size) //Conseguir el promedio

  val vote_count: Seq[(String ,Double)] = Seq(("valores igual a 0" -> votos.count(_ <= "1700").toDouble/1000), //valores igual a 0
    ("valores mayores a 1200" -> votos.count(_ >= "1200").toDouble/1000), //valores mayores a 1200
    "valores igual a 1200" -> votos.count(x => x >= (x.map(_.toDouble).sum/x.size).toString)/1000//valores igual a 1200
  )
  PieChart(vote_count)
    .title("Datos descriptivos")
    .rightLegend()
    .render()
    .write(new File("C:\\Users\\USUARIO\\Downloads\\vote_count.png"))
  println("Gráfico generado")
}
