package PriSpark.TPCH

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType


case class Customer(
 c_custkey: Long,
 c_name: String,
 c_address: String,
 c_nationkey: Long,
 c_phone: String,
 c_acctbal: Double,
 c_mktsegment: String,
 c_comment: String)

case class Lineitem(
 l_orderkey: Long,
 l_partkey: Long,
 l_suppkey: Long,
 l_linenumber: Long,
 l_quantity: Double,
 l_extendedprice: Double,
 l_discount: Double,
 l_tax: Double,
 l_returnflag: String,
 l_linestatus: String,
 l_shipdate: String,
 l_commitdate: String,
 l_receiptdate: String,
 l_shipinstruct: String,
 l_shipmode: String,
 l_comment: String)

case class Nation(
 n_nationkey: Long,
 n_name: String,
 n_regionkey: Long,
 n_comment: String)

case class Orders(
 o_orderkey: Long,
 o_custkey: Long,
 o_orderstatus: String,
 o_totalprice: Double,
 o_orderdate: String,
 o_orderpriority: String,
 o_clerk: String,
 o_shippriority: Long,
 o_comment: String)

case class Part(
 p_partkey: Long,
 p_name: String,
 p_mfgr: String,
 p_brand: String,
 p_type: String,
 p_size: Long,
 p_container: String,
 p_retailprice: Double,
 p_comment: String)

case class Partsupp(
 ps_partkey: Long,
 ps_suppkey: Long,
 ps_availqty: Long,
 ps_supplycost: Double,
 ps_comment: String)

case class Region(
 r_regionkey: Long,
 r_name: String,
 r_comment: String)

case class Supplier(
 s_suppkey: Long,
 s_name: String,
 s_address: String,
 s_nationkey: Long,
 s_phone: String,
 s_acctbal: Double,
 s_comment: String)



class TPCHSchemaProvider_Original(spark: SparkSession, inputDir: String) {

  val time1 = System.currentTimeMillis()

  val customer = spark.read.option("delimiter", "|").schema(ScalaReflection.schemaFor[Customer].dataType.asInstanceOf[StructType]).csv(inputDir + "/customer.tbl")
  val lineitem = spark.read.option("delimiter", "|").schema(ScalaReflection.schemaFor[Lineitem].dataType.asInstanceOf[StructType]).csv(inputDir + "/lineitem.tbl")
  val nation = spark.read.option("delimiter", "|").schema(ScalaReflection.schemaFor[Nation].dataType.asInstanceOf[StructType]).csv(inputDir + "/nation.tbl")
  val region = spark.read.option("delimiter", "|").schema(ScalaReflection.schemaFor[Region].dataType.asInstanceOf[StructType]).csv(inputDir + "/region.tbl")
  val orders = spark.read.option("delimiter", "|").schema(ScalaReflection.schemaFor[Orders].dataType.asInstanceOf[StructType]).csv(inputDir + "/orders.tbl")
  val part = spark.read.option("delimiter", "|").schema(ScalaReflection.schemaFor[Part].dataType.asInstanceOf[StructType]).csv(inputDir + "/part.tbl")
  val partsupp = spark.read.option("delimiter", "|").schema(ScalaReflection.schemaFor[Partsupp].dataType.asInstanceOf[StructType]).csv(inputDir + "/partsupp.tbl")
  val supplier = spark.read.option("delimiter", "|").schema(ScalaReflection.schemaFor[Supplier].dataType.asInstanceOf[StructType]).csv(inputDir + "/supplier.tbl")

  //  customer.createOrReplaceTempView("customer")
  //  lineitem.createOrReplaceTempView("lineitem")
  //  nation.createOrReplaceTempView("nation")
  //  region.createOrReplaceTempView("region")
  //  orders.createOrReplaceTempView("orders")
  //  part.createOrReplaceTempView("part")
  //  partsupp.createOrReplaceTempView("partsupp")
  //  supplier.createOrReplaceTempView("supplier")

  customer.write.mode("overwrite").saveAsTable("customer")
  lineitem.write.mode("overwrite").saveAsTable("lineitem")
  nation.write.mode("overwrite").saveAsTable("nation")
  region.write.mode("overwrite").saveAsTable("region")
  orders.write.mode("overwrite").saveAsTable("orders")
  part.write.mode("overwrite").saveAsTable("part")
  partsupp.write.mode("overwrite").saveAsTable("partsupp")
  supplier.write.mode("overwrite").saveAsTable("supplier")

  val time2 = System.currentTimeMillis()

  //  println(spark.catalog.currentDatabase)
  //  spark.catalog.listDatabases().show(false)

  val tables = Array("customer","lineitem","nation","region","orders","part","partsupp","supplier")
  var num:Long = 0
  for (t <- tables){
    num = num + spark.table(t).count()
  }

  val time3 = System.currentTimeMillis()


  spark.sql("ANALYZE TABLE lineitem COMPUTE STATISTICS")
  spark.sql("ANALYZE TABLE lineitem COMPUTE STATISTICS FOR COLUMNS l_orderkey")
  spark.sql("ANALYZE TABLE lineitem COMPUTE STATISTICS FOR COLUMNS l_partkey")
  spark.sql("ANALYZE TABLE lineitem COMPUTE STATISTICS FOR COLUMNS l_suppkey")
  spark.sql("ANALYZE TABLE lineitem COMPUTE STATISTICS FOR COLUMNS l_linenumber")
  spark.sql("ANALYZE TABLE lineitem COMPUTE STATISTICS FOR COLUMNS l_quantity")
  spark.sql("ANALYZE TABLE lineitem COMPUTE STATISTICS FOR COLUMNS l_extendedprice")
  spark.sql("ANALYZE TABLE lineitem COMPUTE STATISTICS FOR COLUMNS l_discount")
  spark.sql("ANALYZE TABLE lineitem COMPUTE STATISTICS FOR COLUMNS l_tax")
  spark.sql("ANALYZE TABLE lineitem COMPUTE STATISTICS FOR COLUMNS l_returnflag")
  spark.sql("ANALYZE TABLE lineitem COMPUTE STATISTICS FOR COLUMNS l_linestatus")
  spark.sql("ANALYZE TABLE lineitem COMPUTE STATISTICS FOR COLUMNS l_shipdate")
  spark.sql("ANALYZE TABLE lineitem COMPUTE STATISTICS FOR COLUMNS l_commitdate")
  spark.sql("ANALYZE TABLE lineitem COMPUTE STATISTICS FOR COLUMNS l_receiptdate")
  spark.sql("ANALYZE TABLE lineitem COMPUTE STATISTICS FOR COLUMNS l_shipinstruct")
  spark.sql("ANALYZE TABLE lineitem COMPUTE STATISTICS FOR COLUMNS l_shipmode")
  spark.sql("ANALYZE TABLE lineitem COMPUTE STATISTICS FOR COLUMNS l_comment")

  spark.sql("ANALYZE TABLE orders COMPUTE STATISTICS")
  spark.sql("ANALYZE TABLE orders COMPUTE STATISTICS FOR COLUMNS o_orderkey")
  spark.sql("ANALYZE TABLE orders COMPUTE STATISTICS FOR COLUMNS o_custkey")
  spark.sql("ANALYZE TABLE orders COMPUTE STATISTICS FOR COLUMNS o_orderstatus")
  spark.sql("ANALYZE TABLE orders COMPUTE STATISTICS FOR COLUMNS o_totalprice")
  spark.sql("ANALYZE TABLE orders COMPUTE STATISTICS FOR COLUMNS o_orderdate")
  spark.sql("ANALYZE TABLE orders COMPUTE STATISTICS FOR COLUMNS o_orderpriority")
  spark.sql("ANALYZE TABLE orders COMPUTE STATISTICS FOR COLUMNS o_clerk")
  spark.sql("ANALYZE TABLE orders COMPUTE STATISTICS FOR COLUMNS o_shippriority")
  spark.sql("ANALYZE TABLE orders COMPUTE STATISTICS FOR COLUMNS o_comment")

  spark.sql("ANALYZE TABLE customer COMPUTE STATISTICS")
  spark.sql("ANALYZE TABLE customer COMPUTE STATISTICS FOR COLUMNS c_custkey")
  spark.sql("ANALYZE TABLE customer COMPUTE STATISTICS FOR COLUMNS c_name")
  spark.sql("ANALYZE TABLE customer COMPUTE STATISTICS FOR COLUMNS c_address")
  spark.sql("ANALYZE TABLE customer COMPUTE STATISTICS FOR COLUMNS c_nationkey")
  spark.sql("ANALYZE TABLE customer COMPUTE STATISTICS FOR COLUMNS c_phone")
  spark.sql("ANALYZE TABLE customer COMPUTE STATISTICS FOR COLUMNS c_acctbal")
  spark.sql("ANALYZE TABLE customer COMPUTE STATISTICS FOR COLUMNS c_mktsegment")
  spark.sql("ANALYZE TABLE customer COMPUTE STATISTICS FOR COLUMNS c_comment")

  spark.sql("ANALYZE TABLE partsupp COMPUTE STATISTICS")
  spark.sql("ANALYZE TABLE partsupp COMPUTE STATISTICS FOR COLUMNS ps_partkey")
  spark.sql("ANALYZE TABLE partsupp COMPUTE STATISTICS FOR COLUMNS ps_suppkey")
  spark.sql("ANALYZE TABLE partsupp COMPUTE STATISTICS FOR COLUMNS ps_availqty")
  spark.sql("ANALYZE TABLE partsupp COMPUTE STATISTICS FOR COLUMNS ps_supplycost")
  spark.sql("ANALYZE TABLE partsupp COMPUTE STATISTICS FOR COLUMNS ps_comment")

  spark.sql("ANALYZE TABLE part COMPUTE STATISTICS")
  spark.sql("ANALYZE TABLE part COMPUTE STATISTICS FOR COLUMNS p_partkey")
  spark.sql("ANALYZE TABLE part COMPUTE STATISTICS FOR COLUMNS p_name")
  spark.sql("ANALYZE TABLE part COMPUTE STATISTICS FOR COLUMNS p_mfgr")
  spark.sql("ANALYZE TABLE part COMPUTE STATISTICS FOR COLUMNS p_brand")
  spark.sql("ANALYZE TABLE part COMPUTE STATISTICS FOR COLUMNS p_type")
  spark.sql("ANALYZE TABLE part COMPUTE STATISTICS FOR COLUMNS p_size")
  spark.sql("ANALYZE TABLE part COMPUTE STATISTICS FOR COLUMNS p_container")
  spark.sql("ANALYZE TABLE part COMPUTE STATISTICS FOR COLUMNS p_retailprice")
  spark.sql("ANALYZE TABLE part COMPUTE STATISTICS FOR COLUMNS p_comment")

  spark.sql("ANALYZE TABLE supplier COMPUTE STATISTICS")
  spark.sql("ANALYZE TABLE supplier COMPUTE STATISTICS FOR COLUMNS s_suppkey")
  spark.sql("ANALYZE TABLE supplier COMPUTE STATISTICS FOR COLUMNS s_name")
  spark.sql("ANALYZE TABLE supplier COMPUTE STATISTICS FOR COLUMNS s_address")
  spark.sql("ANALYZE TABLE supplier COMPUTE STATISTICS FOR COLUMNS s_nationkey")
  spark.sql("ANALYZE TABLE supplier COMPUTE STATISTICS FOR COLUMNS s_phone")
  spark.sql("ANALYZE TABLE supplier COMPUTE STATISTICS FOR COLUMNS s_acctbal")
  spark.sql("ANALYZE TABLE supplier COMPUTE STATISTICS FOR COLUMNS s_comment")

  spark.sql("ANALYZE TABLE nation COMPUTE STATISTICS")
  spark.sql("ANALYZE TABLE nation COMPUTE STATISTICS FOR COLUMNS n_nationkey")
  spark.sql("ANALYZE TABLE nation COMPUTE STATISTICS FOR COLUMNS n_name")
  spark.sql("ANALYZE TABLE nation COMPUTE STATISTICS FOR COLUMNS n_regionkey")
  spark.sql("ANALYZE TABLE nation COMPUTE STATISTICS FOR COLUMNS n_comment")

  spark.sql("ANALYZE TABLE region COMPUTE STATISTICS")
  spark.sql("ANALYZE TABLE region COMPUTE STATISTICS FOR COLUMNS r_regionkey")
  spark.sql("ANALYZE TABLE region COMPUTE STATISTICS FOR COLUMNS r_name")
  spark.sql("ANALYZE TABLE region COMPUTE STATISTICS FOR COLUMNS r_comment")


  val time4 = System.currentTimeMillis()

  val writeTableTime = time2 - time1
  val allTableCountTime = time3 -time2
  val analyzeAllColTime = time4 - time3


}
