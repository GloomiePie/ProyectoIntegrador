import com.cibo.evilplot.plot._
import com.cibo.evilplot.plot.aesthetics.DefaultTheme._

import com.github.tototoshi.csv.CSVReader

import java.io.File

object AnalisisNumerico extends App{
    val reader = CSVReader.open(new File("C:\\Users\\SALA A\\Downloads\\movie_dataset.csv"))
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

    val budget: Seq[Double] = Seq(presupuesto.maxBy(x => x.toDouble).toDouble,
        presupuesto.minBy(x => x.toDouble).toDouble,
        presupuesto.filter(x => x != "0").minBy(x => x.toDouble).toDouble, (presupuesto.map(x => x.toDouble).sum
          / presupuesto.size).toDouble)

    val medidas = List("Máximo", "Mínimo", "Mínimo sin 0", "Promedio")

    BarChart(budget)
      .title("Datos descriptivos")
      .xAxis(medidas)
      .yAxis()
      .frame()
      .yLabel("BUDGET")
      .bottomLegend()
      .render()
      .write(new File("C:\\Users\\SALA A\\Downloads\\budget.png"))
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

    val popularity : Seq[Double] = Seq(popularidad.maxBy(x => x.toDouble).toDouble,
        popularidad.minBy(x => x.toDouble).toDouble,
        popularidad.filter(x => !x.equals("0.0")).minBy(x => x.toDouble).toDouble,
        popularidad.map(x => x.toDouble).sum / popularidad.size)

    BarChart(popularity)
      .title("Datos descriptivos")
      .xAxis(medidas)
      .yAxis()
      .frame()
      .yLabel("POPULARITY")
      .bottomLegend()
      .render()
      .write(new File("C:\\Users\\SALA A\\Downloads\\budget.png"))
    println("Gráfico generado\n")

    // Columna revenue
    println("Columna revenue")
    val ganacias = data
      .flatMap(elem => elem.get("revenue"))

    printf("La ganacia más alto es: $%s\n", ganacias.maxBy(x => x.toDouble)) //Conseguir la mayor ganacia
    printf("La ganacia más bajo es: $%s\n", ganacias.minBy(x => x.toDouble)) //Conseguir la menor ganacia
    printf("La ganacia más bajo sin 0 es: $%s\n", ganacias.filter(x => !x.equals("0")).minBy(x => x.toDouble)) //Conseguir la menor ganacia sin 0
    printf("La ganacia promedio es: $%.2f\n", ganacias.map(x => x.toDouble).sum
      / ganacias.size) //Conseguir el promedio

    val revenue: Seq[Double] = Seq(ganacias.maxBy(x => x.toDouble).toDouble,
        ganacias.minBy(x => x.toDouble).toDouble,
        ganacias.filter(x => !x.equals("0")).minBy(x => x.toDouble).toDouble,
        ganacias.map(x => x.toDouble).sum / ganacias.size)

    BarChart(revenue)
      .title("Datos descriptivos")
      .xAxis(medidas)
      .yAxis()
      .frame()
      .yLabel("BUDGET")
      .bottomLegend()
      .render()
      .write(new File("C:\\Users\\SALA A\\Downloads\\budget.png"))
    println("Gráfico generado")

    // Columna vote average
    println("Columna vote average")
    val promedio = data
      .flatMap(elem => elem.get("vote_average"))

    printf("El promedio más alto es: %s\n", promedio.maxBy(x => x.toDouble)) //Conseguir la mayor ganacia
    printf("El promedio más bajo es: %s\n", promedio.minBy(x => x.toDouble)) //Conseguir la menor ganacia
    printf("El promedio más bajo sin 0 es: %s\n", promedio.filter(x => !x.equals("0.0")).minBy(x => x.toDouble)) //Conseguir la menor ganacia sin 0
    printf("El promedio es: %.2f\n", promedio.map(x => x.toDouble).sum
      / promedio.size) //Conseguir el promedio

    BarChart(revenue)
      .title("Datos descriptivos")
      .xAxis(medidas)
      .yAxis()
      .frame()
      .yLabel("BUDGET")
      .bottomLegend()
      .render()
      .write(new File("C:\\Users\\SALA A\\Downloads\\budget.png"))
    println("Gráfico generado")

    // Columna vote count
    println("Columna vote average")
    val votos = data
      .flatMap(elem => elem.get("vote_count"))

    printf("El voto más alto es: %s\n", votos.maxBy(x => x.toInt)) //Conseguir la mayor ganacia
    printf("El voto más bajo es: %s\n", votos.minBy(x => x.toInt)) //Conseguir la menor ganacia
    printf("El voto promedio es: %.2f\n", votos.map(x => x.toDouble).sum
      / votos.size) //Conseguir el promedio

}
