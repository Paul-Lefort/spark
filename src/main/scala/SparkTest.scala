import org.apache.spark.sql.SparkSession

object SparkTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark-Scala")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println("Connexion réussie au Master")

    try {
      val data = spark.sparkContext.parallelize(1 to 1000)
      val resultat = data.sum()
      println(s"Somme calculée : $resultat")


      Thread.sleep(100000)

    } catch {
      case e: Exception => println(s"Erreur : ${e.getMessage}")
    } finally {
      spark.stop()
    }

    object Twofer {
      def twofer(name: String): String = ???
      if(name != "")
      print ("One for " + name + ",one for me.")
    }


  }
}