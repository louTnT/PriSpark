package Query

import Query.L2.{laplace, smoothElasticSensitivity}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object L5 {
  /**
   * anti-join
   * @param args
   */

   def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")

    val spark = SparkSession.builder()
    //      .master("local")
    .appName("L5")
    .config("spark.driver.maxResultSize", 0)
    .getOrCreate()

    val sc = spark.sparkContext

    //    val path = "/Users/lou/Documents/IDEAProjects/UnstructuredDP/PigMixBenchmark/src/main/resources/"
    //    val path = "/PigMix/16G/"
    val path = "/PigMix/80G/"
    val pageViewsPath = path + "page_views/"
    val powerUsersPath = path + "power_users/"

    val pageViews = sc.textFile(pageViewsPath)
    val powerUsers = sc.textFile(powerUsersPath)

    val A = pageViews.map(x => x.split(",")(0))

    val B = A.map(x => (x, x))
    //    B.take(5).foreach(println)

    val alpha = powerUsers.map(x => x.split(",")(0))

    val beta = alpha.map(x => (x, x))
    //    beta.take(5).foreach(println)


    val C = B.cogroup(beta)
    val c = C.count()

    println("realNum: ", c)

    

}

}
