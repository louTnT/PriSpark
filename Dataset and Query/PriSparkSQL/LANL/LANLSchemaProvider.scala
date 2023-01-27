package PriSpark.LANL

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType


case class Auth(
 a_time: Long,
 a_srcuser: String,
 a_dstuser: String,
 a_srcpc: String,
 a_dstpc: String,
 a_authType: String,
 a_logonType: String,
 a_authOrient: String,
 a_status: String)

case class Proc(
 p_time: Long,
 p_srcuser: String,
 p_pc: String,
 p_proName: String,
 p_status: String)

case class Flows(
 f_time: Long,
 f_duration: Long,
 f_srcpc: String,
 f_srcport: String,
 f_dstpc: String,
 f_dstport: String,
 f_protocol: Long,
 f_packetCount: Long,
 f_byteCount: Long)

case class Dns(
 d_time: Long,
 d_srcpc: String,
 d_pcResolved: String)

case class Redteam(
 r_time: Long,
 r_srcuser: String,
 r_srcpc: String,
 r_dstpc: String)



class LANLSchemaProvider_Original(spark: SparkSession, inputDir: String) {

  val time1 = System.currentTimeMillis()

  val Auth = spark.read.option("delimiter", ",").schema(ScalaReflection.schemaFor[Auth].dataType.asInstanceOf[StructType]).csv(inputDir + "/auth.txt")
  val Proc = spark.read.option("delimiter", ",").schema(ScalaReflection.schemaFor[Proc].dataType.asInstanceOf[StructType]).csv(inputDir + "/proc.txt")
  val Flows = spark.read.option("delimiter", ",").schema(ScalaReflection.schemaFor[Flows].dataType.asInstanceOf[StructType]).csv(inputDir + "/flows.txt")
  val Dns = spark.read.option("delimiter", ",").schema(ScalaReflection.schemaFor[Dns].dataType.asInstanceOf[StructType]).csv(inputDir + "/dns.txt")
  val Redteam = spark.read.option("delimiter", ",").schema(ScalaReflection.schemaFor[Redteam].dataType.asInstanceOf[StructType]).csv(inputDir + "/redteam.txt")

  Auth.write.mode("overwrite").saveAsTable("auth")
  Proc.write.mode("overwrite").saveAsTable("proc")
  Flows.write.mode("overwrite").saveAsTable("flows")
  Dns.write.mode("overwrite").saveAsTable("dns")
  Redteam.write.mode("overwrite").saveAsTable("redteam")

  val time2 = System.currentTimeMillis()

  //  println(spark.catalog.currentDatabase)
  //  spark.catalog.listDatabases().show(false)

  val tables = Array("auth","proc","flows","dns","redteam")
  var num:Long = 0
  for (t <- tables){
    num = num + spark.table(t).count()
  }

  val time3 = System.currentTimeMillis()


  spark.sql("ANALYZE TABLE auth COMPUTE STATISTICS")
  //  spark.sql("ANALYZE TABLE auth COMPUTE STATISTICS FOR COLUMNS a_time")
  spark.sql("ANALYZE TABLE auth COMPUTE STATISTICS FOR COLUMNS a_srcuser")
  //  spark.sql("ANALYZE TABLE auth COMPUTE STATISTICS FOR COLUMNS a_dstuser")
  //  spark.sql("ANALYZE TABLE auth COMPUTE STATISTICS FOR COLUMNS a_srcpc")
  //  spark.sql("ANALYZE TABLE auth COMPUTE STATISTICS FOR COLUMNS a_dstpc")
  //  spark.sql("ANALYZE TABLE auth COMPUTE STATISTICS FOR COLUMNS a_authType")
  //  spark.sql("ANALYZE TABLE auth COMPUTE STATISTICS FOR COLUMNS a_logonType")
  //  spark.sql("ANALYZE TABLE auth COMPUTE STATISTICS FOR COLUMNS a_authOrient")
  //  spark.sql("ANALYZE TABLE auth COMPUTE STATISTICS FOR COLUMNS a_status")

  //  spark.sql("ANALYZE TABLE proc COMPUTE STATISTICS")
  //  spark.sql("ANALYZE TABLE proc COMPUTE STATISTICS FOR COLUMNS p_time")
  //  spark.sql("ANALYZE TABLE proc COMPUTE STATISTICS FOR COLUMNS p_srcuser")
  //  spark.sql("ANALYZE TABLE proc COMPUTE STATISTICS FOR COLUMNS p_pc")
  //  spark.sql("ANALYZE TABLE proc COMPUTE STATISTICS FOR COLUMNS p_proName")
  //  spark.sql("ANALYZE TABLE proc COMPUTE STATISTICS FOR COLUMNS p_status")

  //  spark.sql("ANALYZE TABLE flows COMPUTE STATISTICS")
  //  spark.sql("ANALYZE TABLE flows COMPUTE STATISTICS FOR COLUMNS f_time")
  //  spark.sql("ANALYZE TABLE flows COMPUTE STATISTICS FOR COLUMNS f_duration")
  //  spark.sql("ANALYZE TABLE flows COMPUTE STATISTICS FOR COLUMNS f_srcpc")
  //  spark.sql("ANALYZE TABLE flows COMPUTE STATISTICS FOR COLUMNS f_srcport")
  //  spark.sql("ANALYZE TABLE flows COMPUTE STATISTICS FOR COLUMNS f_dstpc")
  //  spark.sql("ANALYZE TABLE flows COMPUTE STATISTICS FOR COLUMNS f_dstport")
  //  spark.sql("ANALYZE TABLE flows COMPUTE STATISTICS FOR COLUMNS f_protocol")
  //  spark.sql("ANALYZE TABLE flows COMPUTE STATISTICS FOR COLUMNS f_packetCount")
  //  spark.sql("ANALYZE TABLE flows COMPUTE STATISTICS FOR COLUMNS f_byteCount")

  //  spark.sql("ANALYZE TABLE dns COMPUTE STATISTICS")
  //  spark.sql("ANALYZE TABLE dns COMPUTE STATISTICS FOR COLUMNS d_time")
  //  spark.sql("ANALYZE TABLE dns COMPUTE STATISTICS FOR COLUMNS d_srcpc")
  //  spark.sql("ANALYZE TABLE dns COMPUTE STATISTICS FOR COLUMNS d_pcResolved")

  spark.sql("ANALYZE TABLE redteam COMPUTE STATISTICS")
  //  spark.sql("ANALYZE TABLE redteam COMPUTE STATISTICS FOR COLUMNS r_time")
  //  spark.sql("ANALYZE TABLE redteam COMPUTE STATISTICS FOR COLUMNS r_srcuser")
  spark.sql("ANALYZE TABLE redteam COMPUTE STATISTICS FOR COLUMNS r_srcpc")
  //  spark.sql("ANALYZE TABLE redteam COMPUTE STATISTICS FOR COLUMNS r_dstpc")


  val time4 = System.currentTimeMillis()

  //  spark.sql("DESC EXTENDED lineitem").show(false)
  //  spark.sql("DESC EXTENDED lineitem l_shipdate").show(false)
  //  spark.sql("DESC EXTENDED lineitem l_returnflag").show(false)
  //  spark.sql("DESC EXTENDED lineitem l_linestatus").show(false)


  val writeTableTime = time2 - time1
  val allTableCountTime = time3 -time2
  val analyzeAllColTime = time4 - time3


}
