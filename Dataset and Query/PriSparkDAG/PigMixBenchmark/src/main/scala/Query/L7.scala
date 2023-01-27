package Query

import Query.L2.smoothElasticSensitivity
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object L7 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")

    val spark = SparkSession.builder()
    //      .master("local")
    .appName("L7")
    .config("spark.driver.maxResultSize", 0)
    .getOrCreate()

    val sc = spark.sparkContext

    val path = "/PigMix/80G/"
    val pageViewsPath = path + "page_views/"
    val powerUsersPath = path + "power_users/"

    val pageViews = sc.textFile(pageViewsPath)
    val powerUsers = sc.textFile(powerUsersPath)

    val A = pageViews.map(x => (x.split(",")(0), x.split(",")(5)))
    //    A.take(5).foreach(println)

    val B = A.filter(x => x._2.toLong > 0 ) 
    println(B.count())

    val C = B.groupByKey()
    //    C.take(5).foreach(println)
    val c = C.count()

    println("realNum: ", c)



    /*
register pigperf.jar;
A = load '/user/pig/tests/data/pigmix/page_views' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader() as (user, action, timespent, query_term,
            ip_addr, timestamp, estimated_revenue, page_info, page_links);
B = foreach A generate user, timestamp;
C = group B by user parallel 40;
D = foreach C {
    morning = filter B by timestamp < 43200;
    afternoon = filter B by timestamp >= 43200;
    generate group, COUNT(morning), COUNT(afternoon);
}
store D into 'L7out';
*/


}

}
