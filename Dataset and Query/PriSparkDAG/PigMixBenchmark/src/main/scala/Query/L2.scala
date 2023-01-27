package Query

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.functions.udf

object L2 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")

    val spark = SparkSession.builder()
    //      .master("local")
    .appName("L2")
    .config("spark.driver.maxResultSize", 0)
    .getOrCreate()

    val sc = spark.sparkContext

    //    println(System.currentTimeMillis())

    //    val path = "/Users/lou/Documents/IDEAProjects/UnstructuredDP/src/main/resources/PigMix/"
    //    val path = "/PigMix/16G/"
    val path = "/PigMix/80G/"
    val pageViewsPath = path + "page_views/"
    val powerUsersPath = path + "power_users/"

    val pageViews = sc.textFile(pageViewsPath)
    val powerUsers = sc.textFile(powerUsersPath)

    //    println(pageViews.getNumPartitions)
    //    println(powerUsers.getNumPartitions)


    val A = pageViews.map(x => (x.split(",")(0), x.split(",")(6)))   
    //    A.take(5).foreach(println)

    val filterA = A.filter(x => x._2.toLong > 200000)

    val alpha = powerUsers.map(x => x.split(",")(0)) 
    //    alpha.take(5).foreach(println)
    val aaa = powerUsers.map(x => (x.split(",")(0), 1))


    val beta = alpha.map(x => (x, 1))
    //    beta.take(5).foreach(println)

    //    println("A: " + A.count())
    //    println("beta: " + beta.count())


    val C = A.join(beta).map(x => (x._1, x._2._1))  
    //    C.collect.foreach(println)
    //    val C = beta.join(A).map(x => (x._1, x._2._1))  
    val c = C.count()

    println("realNum: ", c)


    //    A = load '/user/pig/tests/data/pigmix/page_views' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader()
    //    as (user, action, timespent, query_term, ip_addr, timestamp,
        //      estimated_revenue, page_info, page_links);
        //    B = foreach A generate user, estimated_revenue;
        //    alpha = load '/user/pig/tests/data/pigmix/power_users' using PigStorage('\u0001') as (name, phone,
            //      address, city, state, zip);
            //    beta = foreach alpha generate name;
            //    C = join B by user, beta by name using 'replicated' parallel 40;
            //    store C into 'L2out';
        }
    }
