package Query

import Query.L2.{laplace, smoothElasticSensitivity}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

// left outer join
object L13 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")

    val spark = SparkSession.builder()
    //      .master("local")
    .appName("L13")
    .config("spark.driver.maxResultSize", 0)
    .getOrCreate()

    val sc = spark.sparkContext

    val path = "/PigMix/80G/"
    val pageViewsPath = path + "page_views/"
    val powerUsersPath = path + "power_users/"

    val pageViews = sc.textFile(pageViewsPath)
    val powerUsers = sc.textFile(powerUsersPath)

    val A = pageViews.map(x => (x.split(",")(0), x.split(",")(6)))
    //    A.take(5).foreach(println)

    val beta = powerUsers.map(x => (x.split(",")(0), x.split(",")(1)) )
    //    beta.take(5).foreach(println)


    val C = A.leftOuterJoin(beta)
    //    C.take(10).foreach(println)
    val c = C.count()

    println("realNum: ", c)


    /*
    register pigperf.jar;
    A = load '/user/pig/tests/data/pigmix/page_views' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader()
    as (user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links);
    B = foreach A generate user, estimated_revenue;
    alpha = load '/user/pig/tests/data/pigmix/power_users_samples' using PigStorage('\u0001') as (name, phone, address, city, state, zip);
    beta = foreach alpha generate name, phone;
    C = join B by user left outer, beta by name parallel 40;
    store C into 'L13out';

    */
}

}
