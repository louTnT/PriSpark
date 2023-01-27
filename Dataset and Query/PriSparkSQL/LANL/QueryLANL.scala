package PriSpark.LANL

import scala.collection.mutable.ArrayBuffer

class QueryLANL {

  val query = new ArrayBuffer[String]()

  val Q1 = "SELECT " +
  "count(*) as count_num " +
  "FROM " +
  "auth, redteam " +
  "WHERE " +
  "r_srcpc = a_srcpc " +
  "AND a_logonType = 'Network' " +
  "AND a_time < 1094400 " +
  "AND a_time > 1008000 "

  
  val Q2 = "SELECT " +
  "count(*) as count_num " +
  "FROM " +
  "auth, proc " +
  "WHERE " +
  "a_srcuser = p_srcuser " +
  "AND p_proName = 'P16' " +
  "AND p_time < 1094400 " +
  "AND p_time > 1008000 "


  val Q3 = "SELECT " +
  "count(*) as count_num " +
  "FROM " +
  "auth JOIN redteam ON r_srcpc = a_srcpc JOIN flows ON f_srcpc = a_srcpc " +
  "WHERE " +
  "f_protocol = 6 " +
  "AND a_time < 1094400 " +
  "AND a_time > 1008000 "

  



  //  query.append(Q1)

  //  query.append(Q2)

}
