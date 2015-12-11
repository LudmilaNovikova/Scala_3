package com.artezio.novikova.ludmila

import java.util.Date
import java.sql.Date

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by dvlar on 06.12.2015.
 */
object MainCrimes {
  case class Crime(
                    cdatetime: java.sql.Date,
                    address: String,
                    district: String,
                    beat: String,
                    grid: String,
                    crimedescr: String,
                    ucr_ncic_code: Int,
                    latitude: BigDecimal,
                    longitude: BigDecimal)

  def main(args: Array[String]) {
    println("Hi, there")
    val file = getClass.getResource("/SacramentocrimeJanuary2006.csv").getPath
    val conf = new SparkConf().setAppName("Crimes in Sacramento districts calculation")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    try {
      val sqlContext= new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._
      import sqlContext._

      val crimes_str = sc.textFile(file, 2).filter(!_.contains("cdatetime"))
      println("crimes size: " + crimes_str.count())

      val format = new java.text.SimpleDateFormat("MM/dd/yy")

      val crimes = crimes_str.map(r => {
        val elements = r.split(",")
        Crime(new java.sql.Date(format.parse(elements(0)).getTime), elements(1), elements(2), elements(3), elements(4),
          elements(5), elements(6).toInt, BigDecimal(elements(7)), BigDecimal(elements(8)))
      }).cache()

      // crimes using spark sql
      crimes.toDF().registerTempTable("crimes")
      val district_crimes_sql = sql("select district, count(*) as crimes_count from crimes group by district").collect()
      println("Grouped crimes with sql")
      district_crimes_sql.foreach(println(_))

      // crimes using collections api
      val district_crimes = crimes.groupBy(f => f.district).mapValues(_.size).collect()
      println("Grouped crimes with collection api")
      district_crimes.foreach(println(_))
    } finally {
      sc.stop()
    }
  }
}
