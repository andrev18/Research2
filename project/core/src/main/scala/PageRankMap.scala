package main.scala

import scala.collection.mutable

object PageRankMap {
  private val hashmap = new mutable.HashMap[String, Double]()

  def addResult(key: String, value: Double): Unit = {
    hashmap.put(key, value)
  }

  def getValue(key: String): Double = {
    val value = hashmap.get(key)
    if(value == None) 1d else value.get
  }

  def print(): Unit = {
    hashmap.foreach(pair => println(pair._1 + " : " + pair._2))
  }


}
