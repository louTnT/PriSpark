package PriSpark.TPCH

import scala.collection.mutable.ArrayBuffer

class QueryTPCHModify {

  val query = new ArrayBuffer[String]()

  val Q1 = "SELECT " +
  "count(*) as count_order " +
  //    "count(*) " +
  "FROM " +
  "lineitem " +
  "WHERE " +
  "l_extendedprice <= 50000 " +
  "AND l_extendedprice >= 30000" +
  "GROUP BY " +
  "l_returnflag, " +
  "l_linestatus " +
  "ORDER BY " +
  "l_returnflag, " +
  "l_linestatus "


  val Q4 = "SELECT " +
  "count(*) as order_count " +
  "FROM orders " +
  "WHERE " +
  "o_orderdate >= date '1993-07-01' " +
  "AND o_orderdate < date '1993-07-01' + interval '3' month " +
  "AND exists ( " +
  "SELECT * " +
  "FROM " +
  "lineitem " +
  "WHERE " +
  "l_orderkey = o_orderkey " +
  "AND l_quantity = 10 ) " +
  "GROUP BY " +
  "o_orderpriority " +
  "ORDER BY " +
  "o_orderpriority "


  val Q13 = "SELECT " +
  "count(*) as custdist " +
  "FROM ( " +
  "SELECT " +
  "c_custkey, count(o_orderkey) " +
  "FROM " +
  "customer left outer join orders on " +
  "c_custkey = o_custkey " +
  "AND o_totalprice >= 200000 " +
  "AND o_totalprice < 300000 " +
  "GROUP BY " +
  "c_custkey )as c_orders (c_custkey, c_count) " +
  "GROUP BY " +
  "c_count " +
  "ORDER BY " +
  "custdist desc, c_count desc "


  val Q16 = "SELECT " +
  "count(distinct ps_suppkey) as supplier_cnt " +
  "FROM " +
  "partsupp, part " +
  "WHERE " +
  "p_partkey = ps_partkey " +
  "AND p_brand <> 'Brand#45' " +
  "AND p_type not like 'MEDIUM POLISHED%' " +
  "AND p_size in (49, 14, 23, 45, 19, 3, 36, 9) " +
  "AND ps_availqty < 1000 " +
  "AND ps_supplycost < 100 " +
  "AND ps_suppkey not in ( " +
  "SELECT " +
  "s_suppkey " +
  "FROM " +
  "supplier " +
  "WHERE s_comment like '%Customer%Complaints%' ) " +
  "GROUP BY " +
  "p_brand, p_type, p_size " +
  "ORDER BY " +
  "supplier_cnt desc, p_brand, p_type, p_size "


  val Q21 = "SELECT " +
  "count(*) as numwait " +
  "FROM " +
  "supplier, lineitem l1, orders, nation " +
  "WHERE " +
  "s_suppkey = l1.l_suppkey " +
  "AND o_orderkey = l1.l_orderkey " +
  "AND o_orderstatus = 'F' " +
  "AND l1.l_receiptdate > l1.l_commitdate " +
  "AND l1.l_extendedprice < 20000 " +
  "AND exists ( " +
  "SELECT * " +
  "FROM " +
  "lineitem l2 " +
  "WHERE " +
  "l2.l_orderkey = l1.l_orderkey " +
  "AND l2.l_suppkey <> l1.l_suppkey ) " +
  "AND not exists ( " +
  "SELECT * " +
  "FROM " +
  "lineitem l3 " +
  "WHERE " +
  "l3.l_orderkey = l1.l_orderkey " +
  "AND l3.l_suppkey <> l1.l_suppkey " +
  "AND l3.l_receiptdate > l3.l_commitdate " +
  "AND l3.l_quantity = 20 ) " +
  "AND s_nationkey = n_nationkey " +
  "AND n_name = 'SAUDI ARABIA' " +
  "AND s_acctbal < 3000 " +
  "GROUP BY " +
  "s_name " +
  "ORDER BY " +
  "numwait desc, s_name "



  query.append(Q1, Q4, Q13, Q16, Q21)
  //  query.append(Q21)


}
