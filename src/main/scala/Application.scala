import java.text.SimpleDateFormat
import java.util.{Date, Locale, TimeZone}

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD

object Application extends App {

  Logger.getLogger("org").setLevel(Level.OFF)

  /**
    * @param time - timestamp time
    * @return - time in the epoch format
    */
  def epochToDate(time: Long) = {
    val dateFormat = "yyyy-MM-dd HH:mm:ss"
    val simpleDateFormat = new SimpleDateFormat(dateFormat, Locale.getDefault)
    simpleDateFormat.setTimeZone(TimeZone.getDefault)
    simpleDateFormat.format(new Date(time * 1000L))
  }

  val conf = new SparkConf().setAppName("Assignment2").setMaster("local")
  val sc = new SparkContext(conf)
  val file1 = sc.textFile("/home/knoldus/spark-assignment2/file1.txt")
  val file2 = sc.textFile("/home/knoldus/spark-assignment2/file2.txt")
  val fileRdd1 = file1.map(line => line.split("#"))
  val fileRdd2 = file2.map(line => line.split("#"))
  val employeeRdd: RDD[Employee] = fileRdd1.map(e => Employee(e(0).toInt, e(1), e(2), e(3), e(4), e(5)))
  val salesRdd: RDD[Sales] = fileRdd2.map(s => Sales(s(0).toLong, s(1).toInt, s(2).toDouble))

  val newSalesRdd: RDD[(Int, Int, Int, Int, Double)] = salesRdd.map {
    array => {
      val date: String = epochToDate(array.timestamp)
      val dateArray = date.split("-")
      (dateArray(0).toInt, dateArray(1).toInt, dateArray(2).split(" ")(0).toInt, array.customerId, array.salesPrice)
    }
  }

  val newEmployeeRdd: RDD[(Int, String)] = employeeRdd.map {
    array => (array.customerId, array.state)
  }

  val yearlySalesData = newSalesRdd.groupBy {
    tuple => (tuple._1, tuple._5)
  }

  val monthlySalesData = newSalesRdd.groupBy {
    tuple => (tuple._1, tuple._2, tuple._5)
  }

  val dailySalesData = newSalesRdd.groupBy {
    tuple => (tuple._1, tuple._2, tuple._3, tuple._5)
  }
  println(newSalesRdd.collect.toList)
  println(newEmployeeRdd.collect.toList)

}
