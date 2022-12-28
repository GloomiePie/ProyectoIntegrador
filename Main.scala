import com.github.tototoshi.csv._
import java.io.File

object Main extends App{
  val reader = CSVReader.open(new File("C:/Users/USUARIO/Downloads/movie_dataset.csv"))
  val data = reader.allWithHeaders()
  reader.close()

//Columna budget
  println("Columna budget")
  val presupuesto = data
    .flatMap(elem => elem.get("budget"))

  printf("El presupuesto más alto es: $%s\n", presupuesto.maxBy(x => x.toInt))//Conseguir el mayor presupuesto
  printf("El presupuesto más bajo es: $%s\n", presupuesto.minBy(x => x.toInt))//Conseguir el menor presupuesto
  printf("El presupuesto promedio es: $%.2f\n\n", presupuesto.map(x => x.toDouble).sum
    / presupuesto.size)//Conseguir el promedio

  // Columna id
  println("Columna id")
  val id = data
    .flatMap(elem => elem.get("id"))

  printf("El id más alto es: %s\n", id.maxBy(x => x.toInt)) //Conseguir el mayor id
  printf("El id más bajo es: %s\n", id.minBy(x => x.toInt)) //Conseguir el menor id
  printf("El id promedio es: %s\n\n", id.map(x => x.toInt).sum
    / id.size) //Conseguir el promedio

  // Columna id
  println("Columna popularity")
  val popularidad = data
    .flatMap(elem => elem.get("popularity"))

  printf("La popularidad más alto es: %s\n", popularidad.maxBy(x => x.toDouble)) //Conseguir la mayor popularidad
  printf("La popularidad más bajo es: %s\n", popularidad.minBy(x => x.toDouble)) //Conseguir el menor popularidad
  printf("La popularidad promedio es: %.2f\n\n", popularidad.map(x => x.toDouble).sum
    / popularidad.size) //Conseguir el promedio

  // Columna revenue
  println("Columna revenue")
  val ganacias = data
    .flatMap(elem => elem.get("revenue"))

  printf("La ganacia más alto es: $%s\n", ganacias.maxBy(x => x.toDouble)) //Conseguir la mayor ganacia
  printf("La ganacia más bajo es: $%s\n", ganacias.minBy(x => x.toDouble)) //Conseguir la menor ganacia
  printf("La ganacia promedio es: $%.2f\n\n", ganacias.map(x => x.toDouble).sum
    / ganacias.size) //Conseguir el promedio

  // Columna vote average
  println("Columna vote average")
  val promedio = data
    .flatMap(elem => elem.get("vote_average"))

  printf("El promedio más alto es: %s\n", promedio.maxBy(x => x.toDouble)) //Conseguir la mayor ganacia
  printf("El promedio más bajo es: %s\n", promedio.minBy(x => x.toDouble)) //Conseguir la menor ganacia
  printf("El promedio es: %.2f\n\n", promedio.map(x => x.toDouble).sum
    / promedio.size) //Conseguir el promedio

  // Columna vote count
  println("Columna vote average")
  val votos = data
    .flatMap(elem => elem.get("vote_count"))

  printf("El voto más alto es: %s\n", votos.maxBy(x => x.toInt)) //Conseguir la mayor ganacia
  printf("El voto más bajo es: %s\n", votos.minBy(x => x.toInt)) //Conseguir la menor ganacia
  printf("El voto promedio es: %.2f\n", votos.map(x => x.toDouble).sum
    / votos.size) //Conseguir el promedio
}
