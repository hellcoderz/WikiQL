import scala.math.random
import spark._
import SparkContext._
import scala.util.matching.Regex
import scala.collection.mutable.ListBuffer
import scala.io._
import scala.collection.mutable.HashMap
import scala.util.control._

object main {  

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
 
  

  def map1(q:String):List[String] = {
    val spl = q.split(",")
    var arg1 = spl(0).trim.toLowerCase
    var arg2 = spl(2).trim.toLowerCase
    var rel = spl(1).trim.toLowerCase
    var arg1_is_type = "0"
    var arg2_is_type = "0"

    if(rel contains "|")
      rel = rel.replace("|","---")

    if(arg1 != "-")
      arg1 = " " + arg1 + " "
    if(arg2 != "-")
      arg2 = " " + arg2 + " "

    if(arg1 contains "type:"){
      arg1 = " "+arg1.split(":")(1).toUpperCase.trim+" "
      arg1_is_type = "1"
    }
    if(arg2 contains "type:"){
      arg2 = " "+arg2.split(":")(1).toUpperCase.trim+" "
      arg2_is_type = "1"
    }

    List(arg1,rel,arg2,arg1_is_type,arg2_is_type)

  }

  def map2(line:String,arg1:String,rel:String,arg2:String,a1_is_type:String,a2_is_type:String):Boolean = {
    val words = line.split("\t")
    if(a1_is_type == "0" && ((" "+words(1)+" ").toLowerCase contains arg1) || arg1 == "-"){

      if(a2_is_type == "0" && ((" "+words(3)+" ").toLowerCase contains arg2) || arg2 == "-"){
        if(rel contains "---"){
          val rels = rel.split("---")
          //println("with or"+rels)
          for(r <- rels){
            if(words(2) contains "---"){
              for(word <- words(2).split("---")){
                if((" "+word+" ").toLowerCase contains (" "+r+" ")){
                  return true
                }
              }
            }else{

                if((" "+words(2)+" ").toLowerCase contains (" "+r+" ")){
                  return true
                }
            }
          }
        }else{
         // println("no or"+rel)
          if(words(2) contains "---"){
              for(word <- words(2).split("---")){
                //println(word)
                if(((" "+word+" ").toLowerCase contains (" "+rel+" ")) || rel == "-"){
                  return true
                }
              }
          }else{
            if(((" "+words(2)+" ").toLowerCase contains (" "+rel+" ")) || rel == "-"){
              return true
            }
          }
        }
      }else if(a2_is_type == "1" && (words(3) contains "<type=") && ((" "+words(3)+" ") contains arg2) || arg2 == "-"){
        if(rel contains "---"){
          val rels = rel.split("---")
          for(r <- rels){
            if(words(2) contains "---"){
              for(word <- words(2).split("---")){
                if((" "+word+" ").toLowerCase contains (" "+r+" ")){
                  return true
                }
              }
            }else{
                if((" "+words(2)+" ").toLowerCase contains (" "+r+" ")){
                  return true
                }
            }
          }
        }else{
          if(words(2) contains "---"){
              for(word <- words(2).split("---")){
                if(((" "+word+" ").toLowerCase contains (" "+rel+" ")) || rel == "-"){
                  return true
                }
              }
          }else{
            if(((" "+words(2)+" ").toLowerCase contains (" "+rel+" ")) || rel == "-"){
              return true
            }
          }
        }
      }
    }else if(a1_is_type == "1" && (words(1) contains "<type=") && ((" "+words(1)+" ") contains arg1) || arg1 == "-"){
      //println(line)
      if(a2_is_type == "0" && ((" "+words(3)+" ").toLowerCase contains arg2) || arg2 == "-"){
        if(rel contains "---"){
          val rels = rel.split("---")
          for(r <- rels){
            if(words(2) contains "---"){
              for(word <- words(2).split("---")){
                if((" "+word+" ").toLowerCase contains (" "+r+" ")){
                  return true
                }
              }
            }else{
                if((" "+words(2)+" ").toLowerCase contains (" "+r+" ")){
                  return true
                }
            }
          }
        }else{
          if(words(2) contains "---"){
              for(word <- words(2).split("---")){
                if(((" "+word+" ").toLowerCase contains (" "+rel+" ")) || rel == "-"){
                  return true
                }
              }
          }else{
            if(((" "+words(2)+" ").toLowerCase contains (" "+rel+" ")) || rel == "-"){
              return true
            }
          }
        }
      }else if(a2_is_type == "1" && (words(3) contains "<type=") && ((" "+words(3)+" ") contains arg2) || arg2 == "-"){
        if(rel contains "---"){
          val rels = rel.split("---")
          for(r <- rels){
            if(words(2) contains "---"){
              for(word <- words(2).split("---")){
                if((" "+word+" ").toLowerCase contains (" "+r+" ")){
                  return true
                }
              }
            }else{
                if((" "+words(2)+" ").toLowerCase contains (" "+r+" ")){
                  return true
                }
            }
          }
        }else{
          if(words(2) contains "---"){
              for(word <- words(2).split("---")){
                if(((" "+word+" ").toLowerCase contains (" "+rel+" ")) || rel == "-"){
                  return true
                }
              }
          }else{
            if(((" "+words(2)+" ").toLowerCase contains (" "+rel+" ")) || rel == "-"){
              return true
            }
          }
        }
      }
    }
    return false
  }


  def map3(values:Seq[String]):String = {
    if(values.length == 1)
      return values(0)

    val val_map = new HashMap[String,Int]
    for(v <- values){
        //println(v)
        val_map += v -> 0
    }
    var min = 9999
    var value = ""
    for(v1 <- values){
      for(v2 <- values){
        val_map(v1) = val_map(v1) + similarity(v1,v2)
        if(val_map(v1) < min){
          min = val_map(v1)
          value = v1
        }
      }
    }
    (value)

  }

  def map4(line:String):Tuple2[String,String] = {
    val words = line.split("\t")
    val pat = "<type=.*>(.*)</type>".r
    val word1 = pat.replaceAllIn(words(1),"$1").trim.toLowerCase
    val word3 = pat.replaceAllIn(words(3),"$1").trim

    (word1,words(2).trim+"<>"+word3)
  }

  def relation(query:String,input:RDD[String]):RDD[(String,String)] = {
    
    val query_string:String = query
    val query_lst:List[String] = map1(query_string)
    val arg1:String = query_lst(0)
    val arg2:String = query_lst(2)
    val rel:String = query_lst(1)
    val a1_is_type:String = query_lst(3)
    val a2_is_type:String = query_lst(4)
    val data = input.filter(line => map2(line,arg1,rel,arg2,a1_is_type,a2_is_type)).cache()
    val data_m = data.map(line => map4(line)).cache()
    val data_g = data_m.groupByKey().cache()
    val data_r = data_g.mapValues(values => map3(values)).cache()
    return data_r
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
    val inputUS = sc.textFile("../data/reverbed.txt")
    val input = sc.textFile("../data/final.txt")
