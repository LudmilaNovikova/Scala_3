package com.artezio.novikova.ludmila

import java.util.Date

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by dvlar on 06.12.2015.
 */
object MainCrimes {
  def main(args: Array[String]) {
    println("Hi, there")
    val url=getClass.getResource("/SacramentocrimeJanuary2006.csv")
    val file = url.getPath
//    val file = "C:\\Users\\lnovikova\\Downloads\\SacramentocrimeJanuary2006.csv"
    val conf = new SparkConf().setAppName("Crimes in Sacramento districts calculation")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    try{
      val crimes_str = sc.textFile(file, 2).cache().filter(!_.contains("cdatetime"))
      println("crimes size: " + crimes_str.count())

      case class Crime(
                        cdatetime: Date,
                        address: String,
                        district: String,
                        beat: String,
                        grid: String,
                        crimedescr: String,
                        ucr_ncic_code: Int,
                        latitude: BigDecimal,
                        longitude: BigDecimal)

      val format = new java.text.SimpleDateFormat("MM/dd/yy")

      val crimes = crimes_str.map(r => { val elements = r.split(",")
        Crime(format.parse(elements(0)), elements(1), elements(2), elements(3), elements(4),
          elements(5), elements(6).toInt, BigDecimal(elements(7)), BigDecimal(elements(8)))})
      val district_crimes = crimes.groupBy(f => f.district).mapValues(_.size).collect()
      district_crimes.foreach(println(_))
     }finally {
      sc.stop()
    }
//    Thread.sleep(10000)
  }
}
