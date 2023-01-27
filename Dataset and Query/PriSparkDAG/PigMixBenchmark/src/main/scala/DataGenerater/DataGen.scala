package DataGenerater

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object DataGen {
  def main(args:Array[String]) = {
    Logger.getLogger("org").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")

    val spark = SparkSession.builder()
//      .master("local")
      .appName("DataGen")
      .config("spark.sql.warehouse.dir", "/spark/spark-warehouse/PigMix") //路径要设成Hadoop下
      .getOrCreate()

    val partitions = 1
    val dataper  = 500000000
  //val dataper  = 100000000
//    val dataFile = "/PigMix/16G/"
    val dataFile = "/PigMix/80G/"
    val page = dataFile + "page_views"
    val users = dataFile + "power_users"

    val sc = spark.sparkContext
    sc.parallelize(Seq[Int]() , partitions).mapPartitions { _ =>
      (1 to dataper).flatMap{_ =>
        var list = List[String]()
        list = s"""${getKey()},${getThreeLetter()},${getTime()},${getInfoString(15)},${getIp()},${getTime()},${getRevenue()},${getInfoString(20)},${getInfoString(20)}""" :: list
        list}.iterator}.saveAsTextFile(page)

    sc.parallelize(Seq[Int]() , partitions).mapPartitions { _ =>
//      (1 to dataper/100).flatMap{_ =>
      (1 to dataper/10).flatMap{_ =>
        var list = List[String]()
        list = s"""${getKey()},${getPhoneNumber()},${getInfoString(20)},${getInfoString(10)},${getInfoString(2)},${zipcode}""" :: list
        list}.iterator}.saveAsTextFile(users)

    println("Over!")
  }


  def randomStringFromCharList(length: Int, chars: Seq[Char]): String = {
    val sb = new StringBuilder
    for (i <- 1 to length) {
      val randomNum = util.Random.nextInt(chars.length)
      sb.append(chars(randomNum))
    }
    sb.toString
  }

  def randomString(length: Int) = {
    val chars = ('a' to 'z') ++ ('A' to 'Z')
    randomStringFromCharList(length, chars)
  }
  def getThreeLetter(): String ={
    return randomString(3)
  }
  def getTime(): Long = {
    Random.nextLong();
  }
  def getIp(): String ={
    return Random.nextInt(1000) +"."+Random.nextInt(1000) +"."+Random.nextInt(1000) +"."+Random.nextInt(1000)
  }
  def getRevenue(): String ={
    Random.nextInt(1000000).toString
  }
  def getInfoString(n:Int): String ={
    randomString(n)
  }
  def getPhoneNumber(): Int = {
    100000000 + Random.nextInt(899999999)
  }

  def getKey(): String ={
    return randomString(2) + Random.nextInt(100)
  }
  def zipcode = "9" + "0"+ "0" + Random.nextInt(10).toString + Random.nextInt(10).toString

}

