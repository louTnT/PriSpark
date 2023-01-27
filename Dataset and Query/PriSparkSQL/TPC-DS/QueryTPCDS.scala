package PriSpark.TPCDS

import scala.collection.mutable.ArrayBuffer

class QueryTPCDSModify {

  val query = new ArrayBuffer[String]()

  val Q06 = "SELECT " +
  "a.ca_state state, " +
  "count(*) cnt " +
  "FROM " +
  "customer_address a , customer c , store_sales s , date_dim d , item i " +
  "WHERE " +
  "a.ca_address_sk = c.c_current_addr_sk " +
  "AND c.c_customer_sk = s.ss_customer_sk " +
  "AND s.ss_sold_date_sk = d.d_date_sk " +
  "AND s.ss_item_sk = i.i_item_sk " +
  "AND d.d_month_seq = " +
  "(SELECT " +
  "DISTINCT (d_month_seq) " +
  "FROM " +
  "date_dim " +
  "WHERE " +
  "d_year = 2001 " +
  "AND d_moy = 1 ) " +
  "AND i.i_current_price > 1.2 " +
  //    "(SELECT " +
  //    "avg(j.i_current_price) " +
  //    "FROM " +
  //    "item j" +
  //    "WHERE " +
  //    "j.i_category = i.i_category) " +
  "AND s.ss_quantity <= 50 " +
  "AND s.ss_quantity >= 45 " +
  "AND c.c_birth_year > 1973 " +
  "GROUP BY " +
  "a.ca_state " +
  "HAVING " +
  "count(*) >= 10 " +
  "ORDER BY " +
  "cnt, a.ca_state " +
  "LIMIT 100 "

  
  val Q69 = "SELECT " +
  "cd_gender, cd_marital_status, cd_education_status, count(*) cnt1, cd_purchase_estimate, count(*) cnt2, cd_credit_rating, count(*) cnt3 " +
  "FROM " +
  "customer c, customer_address ca, customer_demographics " +
  "WHERE " +
  "c.c_current_addr_sk = ca.ca_address_sk " +
  "AND ca_state IN ('KY', 'GA', 'NM') " +
  "AND cd_demo_sk = c.c_current_cdemo_sk " +
  "AND c.c_birth_year = 1973 " +
  "AND EXISTS (" +
  "SELECT " +
  "* " +
  "FROM " +
  "store_sales, date_dim " +
  "WHERE " +
  "c.c_customer_sk = ss_customer_sk " +
  "AND ss_sold_date_sk = d_date_sk " +
  "AND d_year = 2001 " +
  "AND d_moy BETWEEN 4 AND 4+2 " +
  "AND ss_quantity <= 50 " +
  "AND ss_quantity >= 45 ) " +
  "AND " +
  "(NOT EXISTS " +
  "(SELECT " +
  "* " +
  "FROM " +
  "web_sales, date_dim " +
  "WHERE " +
  "c.c_customer_sk = ws_bill_customer_sk " +
  "AND ws_sold_date_sk = d_date_sk " +
  "AND d_year = 2001 " +
  "AND d_moy BETWEEN 4 AND 4+2 " +
  "AND ws_quantity <= 50 " +
  "AND ws_quantity >= 45 ) " +
  "AND NOT EXISTS " +
  "(SELECT " +
  "* " +
  "FROM " +
  "catalog_sales, date_dim " +
  "WHERE " +
  "c.c_customer_sk = cs_ship_customer_sk " +
  "AND cs_sold_date_sk = d_date_sk " +
  "AND d_year = 2001 " +
  "AND d_moy BETWEEN 4 AND 4+2 " +
  "AND cs_quantity <= 50 " +
  "AND cs_quantity >= 45 )) " +
  "GROUP BY " +
  "cd_gender, " +
  "cd_marital_status, " +
  "cd_education_status, " +
  "cd_purchase_estimate, " +
  "cd_credit_rating " +
  "ORDER BY " +
  "cd_gender, " +
  "cd_marital_status, " +
  "cd_education_status, " +
  "cd_purchase_estimate, " +
  "cd_credit_rating " +
  "LIMIT 100 "

  
  val Q96 = "SELECT " +
  "count(*) " +
  "FROM " +
  "store_sales, household_demographics, time_dim, store " +
  "WHERE " +
  "ss_sold_time_sk = time_dim.t_time_sk " +
  "AND ss_hdemo_sk = household_demographics.hd_demo_sk " +
  "AND ss_store_sk = s_store_sk " +
  "AND time_dim.t_hour = 20 " +
  "AND time_dim.t_minute >= 30 " +
  "AND household_demographics.hd_dep_count = 7 " +
  "AND store.s_store_name = 'ese' " +
  "AND ss_quantity <= 50 " +
  "AND ss_quantity >= 45 "
  "ORDER BY " +
  "count(*) " +
  "LIMIT 100 "

  


  //  query.append(Q06)

  query.append(Q96)



}
