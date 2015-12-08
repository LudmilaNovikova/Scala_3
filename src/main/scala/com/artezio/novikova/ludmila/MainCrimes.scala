package com.artezio.novikova.ludmila

import java.util.Date

import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.DateTime

/**
 * Created by dvlar on 06.12.2015.
 */
object MainCrimes {
  def main(args: Array[String]) {
    val file = "/guest/SacramentocrimeJanuary2006.csv"
    val conf = new SparkConf().setAppName("Crimes in Sacramento districts calculation")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val crimes_str = sc.textFile(file, 2).cache()

    case class Crime(
                      cdatetime: DateTime,
                      address: String,
                      district: String,
                      beat: Int,
                      grid: String,
                      crimedescr: String,
                      ucr_ncic_code: Int,
                      latitude: BigDecimal,
                      longitude: BigDecimal)


    val crimes = crimes_str.map(_.split(",")).map(r => Crime(DateTime.parse(r(0)), r(1), r(2), r(3).toInt, r(4),
      r(5), r(6).toInt, BigDecimal(r(7)), BigDecimal(r(8))))
    val district_crimes = crimes.groupBy(f => f.district).mapValues(_.sum)


  }
}
