import scala.math.random
import spark._
import SparkContext._
import scala.util.matching.Regex
import scala.collection.mutable.ListBuffer
import scala.io._
import scala.collection.mutable.HashMap
import scala.util.control._

object query1 {  

  def min(nums: Int*): Int = nums.min


//Levenshtein distance function for string similarity
  def similarity(str1: String, str2: String): Int = {
    val lenStr1 = str1.length
    val lenStr2 = str2.length
 
    val d: Array[Array[Int]] = Array.ofDim(lenStr1 + 1, lenStr2 + 1)
 
    for (i <- 0 to lenStr1) d(i)(0) = i
    for (j <- 0 to lenStr2) d(0)(j) = j
 
    for (i <- 1 to lenStr1; j <- 1 to lenStr2) {
      val cost = if (str1(i - 1) == str2(j-1)) 0 else 1
 
      d(i)(j) = min(
        d(i-1)(j  ) + 1,     // deletion
        d(i  )(j-1) + 1,     // insertion
        d(i-1)(j-1) + cost   // substitution
      )
    }
 
    d(lenStr1)(lenStr2)
  }
  
def findByTypeAndCategoryAND(line:String,tpe:String,category:String):Boolean = {
      val loop = new Breaks;
      var ccount = 0
      var tcount = 0
      val tq = tpe.split(":")
      val cq = category.split(":")
      val words = line.split("\t")
      for(i <- 1 to words.length - 1){
        val tcwords = words(i).split("<>")
        if(tcwords(0) == "category"){
          for(c <- 0 to cq.length - 1){
            loop.breakable{
              for(tc <- 1 to tcwords.length - 1){
                if((tcwords(tc).toUpperCase.trim).matches(".*"+cq(c).toUpperCase.trim+".*")){
                  ccount = ccount + 1
                  loop.break;

                }

              }
            }
          }
        }else if(tcwords(0) == "type"){
          for(t <- 0 to tq.length - 1){
            loop.breakable{
              for(tc <- 1 to tcwords.length - 1){
                if((tcwords(tc).toUpperCase.trim).matches(".*"+tq(t).toUpperCase.trim+".*")){
                  tcount = tcount + 1
                  loop.break;
                }

              }
            }
          }
        }
      }

      if(tpe.trim == "-" && (ccount == cq.length)){
        return true
      }else if(category.trim == "-" && (tcount == tq.length)){
        return true
      }else if((ccount == cq.length) && (tcount == tq.length)){
        return true
      }

      return false
    
  }

  def findByTypeAndCategoryOR(line:String,tpe:String,category:String):Boolean = {
      val tq = tpe.split(":")
      val cq = category.split(":")
      val words = line.split("\t")
      for(i <- 1 to words.length - 1){
        val tcwords = words(i).split("<>")
        if(tcwords(0) == "category"){
          for(c <- 0 to cq.length - 1){
              for(tc <- 1 to tcwords.length - 1){
                if((tcwords(tc).toUpperCase.trim).matches(".*"+cq(c).toUpperCase.trim+".*")){
                  return true

                }

              }
            
          }
        }else if(tcwords(0) == "type"){
          for(t <- 0 to tq.length - 1){
              for(tc <- 1 to tcwords.length - 1){
                if((tcwords(tc).toUpperCase.trim).matches(".*"+tq(t).toUpperCase.trim+".*")){
                  return true
                }

              }
            
          }
        }
      }

      return false
  }

  def findOtherAND(line:String,key:String,value:String):Boolean = {
    val words = line.split("\t")
    for(i <- 1 to words.length - 1){
      val tcwords = words(i).split("<>")
      if(tcwords(0).trim == key.toLowerCase.trim){
        for(k <- 1 to tcwords.length - 1){
          if((tcwords(k).toLowerCase.trim).matches(value.toLowerCase.trim)){
            return true
          }
        }
      }
    }
    return false
  }

  def findOtherOR(line:String,key:String,value:String):Boolean = {
    val words = line.split("\t")
    for(i <- 1 to words.length - 1){
      val tcwords = words(i).split("<>")
      if(tcwords(0).trim == key.toLowerCase.trim){
        for(k <- 1 to tcwords.length - 1){
          if(tcwords(k).toLowerCase.trim == value.toLowerCase.trim){
            return true
          }
        }
      }
    }
    return false
  }

  def mapstokey(line:String):Tuple2[String,String] = {
    val words = line.split("\t")
    (words(0).trim.toLowerCase,"key")
  }
 

  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: query input_path query_string <master>")
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "query",  System.getenv("SPARK_HOME"), List(System.getenv("SPARK_TEST")))
    //val input = sc.textFile("../data/test.txt")
    val input = sc.textFile("../data/final.txt")
    val out2 = input.filter(line => findByTypeAndCategoryOR(line,"Actor:Adultactor","Actor") )
    val out1 = out2.filter(line => findByTypeAndCategoryAND(line,"-","1990 births:Living people") )
    val out = out1.filter(line => findOtherAND(line,"birthplace","New York"))

    out.saveAsTextFile("out1")
    System.exit(0)
  }
}