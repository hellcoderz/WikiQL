import scala.math.random
import spark._
import SparkContext._
import scala.util.matching.Regex
import scala.collection.mutable.ListBuffer
import scala.io._

object query {  

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
                println(word)
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
      println(line)
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

  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: query input_path query_string <master>")
      System.exit(1)
    }
    val spark = new SparkContext(args(3), "query",  System.getenv("SPARK_HOME"), List(System.getenv("SPARK_TEST")))
    
    val input = spark.textFile(args(0))
    val query_string:String = args(1)
    val query_lst:List[String] = map1(query_string)
    val arg1:String = query_lst(0)
    val arg2:String = query_lst(2)
    val rel:String = query_lst(1)
    val a1_is_type:String = query_lst(3)
    val a2_is_type:String = query_lst(4)
    println(arg1)
    println(rel)
    println(arg2)
    val data = input.filter(line => map2(line,arg1,rel,arg2,a1_is_type,a2_is_type)).cache()

    data.saveAsTextFile(args(2))
    System.exit(0)
  }
}