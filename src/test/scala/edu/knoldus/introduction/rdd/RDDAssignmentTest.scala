package edu.knoldus.introduction.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}

class RDDAssignmentTest extends FunSuite with BeforeAndAfter {

  private val master = "local[4]"
  private val appName = "RDD Assignment"
  private var sc: SparkContext = _
  private var pageCountsRdd: RDD[String] = _

  import PageCounter._

  before {
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    sc = new SparkContext(conf)
    pageCountsRdd = sc.textFile("src/test/resources/testPages.csv")
  }

  test("RDDAssignment Test Scenarios") {
    assert(extractHeadByCount(pageCountsRdd, 1).apply(0) == "aa 112_f.Kr 1 4606")
    assert((filterByString(pageCountsRdd, "/en/").count) == 2)

    val reducedRDD = reduceByKeyWithFunction(pageCountsRdd, (a: Int, b: Int) => a + b)

    val moreViewsRDD = filterByFunction(reducedRDD, (x => (x._2 > 1)))

    assert(moreViewsRDD.filter(x => x._1 == "Main_Page").take(1).apply(0)._2 == 8)

    assert((filterByFunction(reducedRDD, (x => (x._2 > 1))).count) == 2)

  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }


}
