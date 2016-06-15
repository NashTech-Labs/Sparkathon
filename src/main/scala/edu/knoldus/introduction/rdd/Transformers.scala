package edu.knoldus.introduction.rdd

import org.apache.spark.{SparkConf, SparkContext}

object Transformers extends App {

  val conf = new SparkConf().setAppName("BigApple").setMaster("local")
  val sc = new SparkContext(conf)

  /*
    val starterRDD = sc.parallelize(List("A", "AB", "C", "D", "E", "F"))

    val filteredRDD = starterRDD filter (x => x.startsWith("A"))

    val filteredArray = filteredRDD.collect()

    filteredRDD foreach println
  */

  /* // map
   val mappedRDD = starterRDD.map(x => x.toLowerCase())

   starterRDD foreach println
   mappedRDD foreach println



   val starterRDDInt = sc.parallelize(List(List(1, 2), List(3, 4), List(5, 6)))

   val flatMapRDD = starterRDDInt flatMap (a => a map (x => x + 1))

   flatMapRDD foreach println*/

  //mapPartition


  /*val starterRDD2 = sc.parallelize(List(1, 2, 3, 4, 5, 6, 4), 3)
  val mapPartitionedRDD = starterRDD2 mapPartitionsWithIndex ((index, iterator) => {
    val l = iterator.toList
    val yal = l map (x => (x + 1).toString + "->" + index)
    yal.iterator
  })

  mapPartitionedRDD foreach println

  val cmpr = mapPartitionedRDD.collect()
  cmpr foreach println*/


  /* val a = sc.parallelize(1 to 100,6)

   val result = a.reduce((x, y) => x + y)

   println(result)

 */

  /*val b = a.sample(false, 1, 2).collect()
  b foreach println
*/

  val broadcastVar = sc.broadcast(Array(1, 2, 3))
  broadcastVar.value


  val accum = sc.accumulator(0)

  val anRDD = sc.parallelize(1 to 10)

  val transformedRDD = anRDD map (x => {
    accum += 1
    println(s" in tranformation $accum")
    x + accum.value
  })

  //transformedRDD foreach println

  println(accum)

  println(anRDD.reduce((x, y) => {
    accum += 1
    println(s" in action $accum")
    x + y + accum.value
  }))

  println(accum.value)
}
